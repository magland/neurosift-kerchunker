import time
import urllib.request
import urllib.error
import filelock
import json
import multiprocessing
from typing import List
import os
import tempfile
from pydantic import BaseModel
import boto3
import dandi.dandiarchive as da
import lindi

# warning: don't use force=True when running more than one instance of this script because the locking won't do the right thing
force = False


def main():
    dandi_lindi(
        max_time_sec=60 * 60 * 6,
        max_time_sec_per_dandiset=60 * 1
    )


# thisdir = os.path.dirname(os.path.realpath(__file__))
# with open(f'{thisdir}/dandiset_ids.txt', 'r') as f:
#     dandiset_ids = f.read().splitlines()
#     dandiset_ids = [x for x in dandiset_ids if x and not x.startswith('#')]


def dandi_lindi(
    max_time_sec: float,
    max_time_sec_per_dandiset: float
):
    if os.environ.get("AWS_ACCESS_KEY_ID") is None:
        raise ValueError("AWS_ACCESS_KEY_ID not set.")
    if os.environ.get("AWS_SECRET_ACCESS_KEY") is None:
        raise ValueError("AWS_SECRET_ACCESS_KEY not set.")
    if os.environ.get("S3_ENDPOINT_URL") is None:
        raise ValueError("S3_ENDPOINT_URL not set.")

    dandisets = fetch_all_dandisets()

    num_parallel = 6

    timer = time.time()
    for dandiset_index, dandiset in enumerate(dandisets):
        # for dandiset_id in dandiset_ids:
        # dandiset = next((x for x in dandisets if x.dandiset_id == dandiset_id), None)
        # if dandiset is None:
        #     print(f"Dandiset {dandiset_id} not found.")
        #     continue
        print("")
        print(f"Processing {dandiset.dandiset_id} version {dandiset.version} (dandiset {dandiset_index + 1} / {len(dandisets)})")
        if dandiset.dandiset_id not in ['000003', '000019', '000021', '000022', '000028', '000034', '000041', '000044', '000048', '000055', '000056', '000059', '000061', '000065', '000067', '000070', '000114', '000115', '000149', '000165', '000166', '000213', '000218', '000223', '000230', '000233', '000248', '000253', '000294', '000299', '000339', '000363', '000397', '000398', '000399', '000410', '000411', '000447', '000458', '000463', '000465', '000473', '000481', '000482', '000546', '000552', '000554', '000568', '000574', '000575', '000576', '000582', '000618', '000623', '000629', '000673', '000687', '000696', '000710', '000713', '000717', '000732', '000876', '000932', '000935', '000937', '000957', '000960']:
            # for testing, only process select dandisets
            continue
        with multiprocessing.Pool(num_parallel) as p:
            async_results = [
                p.apply_async(handle_dandiset, args=(dandiset.dandiset_id, max_time_sec_per_dandiset, num_parallel, ii))
                for ii in range(num_parallel)
            ]
            for async_result in async_results:
                async_result.get()
        elapsed_sec = time.time() - timer
        print(f"Time elapsed thus far: {elapsed_sec} seconds")
        if elapsed_sec > max_time_sec:
            print(f"Time limit reached for dandiset {dandiset.dandiset_id}.")
            break


def handle_dandiset(
    dandiset_id: str,
    max_time_sec: float,
    modulus: int,
    modulus_offset: int
):
    timer = time.time()

    # Create the dandi parsed url
    parsed_url = da.parse_dandi_url(f"https://dandiarchive.org/dandiset/{dandiset_id}")

    with parsed_url.navigate() as (client, dandiset, assets):
        if dandiset is None:
            print(f"Dandiset {dandiset_id} not found.")
            return

        num_consecutive_not_nwb = 0
        asset_index = 0
        num_assets_processed = 0
        # important to respect the iterator so we don't pull down all the assets at once
        # and overwhelm the server
        for asset_obj in dandiset.get_assets('path'):
            if not asset_obj.path.endswith(".nwb"):
                num_consecutive_not_nwb += 1
                if num_consecutive_not_nwb >= 20:
                    # For example, this is important for 000026 because there are so many non-nwb assets
                    print("Skipping dandiset because too many consecutive non-NWB files.")
                    return
                continue
            else:
                num_consecutive_not_nwb = 0
            if asset_index % modulus != modulus_offset:
                asset_index += 1
                continue
            asset = {
                "identifier": asset_obj.identifier,
                "path": asset_obj.path,
                "size": asset_obj.size,
                "download_url": asset_obj.download_url,
                "dandiset_id": dandiset_id,
            }
            try:
                process_asset(asset, num=asset_index)
            except Exception as e:
                print(asset['download_url'])
                print(f"Error processing asset {asset_index} ({asset['identifier']}): {e}")
                raise
            num_assets_processed += 1
            elapsed_sec = time.time() - timer
            if elapsed_sec > max_time_sec:
                print("Time limit reached.")
                return
            asset_index += 1
        print(f"Processed {num_assets_processed} assets in dandiset {dandiset_id}")


def process_asset(asset, *, num: int):
    dandiset_id = asset['dandiset_id']
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        region_name="auto",  # for cloudflare
    )
    asset_id = asset['identifier']
    file_key = f'dandi/dandisets/{dandiset_id}/assets/{asset_id}/nwb.lindi.json'
    old_zarr_json_file_key = f'dandi/dandisets/{dandiset_id}/assets/{asset_id}/zarr.json'
    info_file_key = f'dandi/dandisets/{dandiset_id}/assets/{asset_id}/info.json'
    lindi_json_url = f'https://lindi.neurosift.org/{file_key}'
    old_zarr_json_url = f'https://lindi.neurosift.org/{old_zarr_json_file_key}'
    info_url = f'https://lindi.neurosift.org/{info_file_key}'
    if not force:
        if _remote_file_exists(lindi_json_url) and _remote_file_exists(info_url):
            info = _download_json(info_url)
            generation_metadata = info.get("generationMetadata", {})
            if generation_metadata.get("generatedBy") == "dandi_lindi":
                if generation_metadata.get("generatedByVersion") == 11:
                    # print(f"Skipping {asset_id} because it already exists.")
                    return
        elif _remote_file_exists(old_zarr_json_url):
            # copying old zarr.json to new nwb.lindi.json
            print(f"Copying {old_zarr_json_url} to {lindi_json_url}")
            s3.copy_object(
                Bucket="neurosift-lindi",
                CopySource={"Bucket": "neurosift-lindi", "Key": old_zarr_json_file_key},
                Key=file_key,
            )
            # deleting old zarr.json
            print(f"Deleting {old_zarr_json_url}")
            s3.delete_object(Bucket="neurosift-lindi", Key=old_zarr_json_file_key)
            return

    print(f"Processing asset {asset['download_url']}")

    lock = acquire_lock(asset_id)
    if lock is None:
        print(f"Skipping {asset_id} because it is locked.")
        return
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            print(f"[{asset['dandiset_id']} {num}] Processing asset {asset_id}: {asset['path']}")
            timer0 = time.time()
            with write_console_output(tmpdir + "/output.txt"):
                _create_lindi_json(asset['download_url'], tmpdir + "/nwb.lindi.json")
            with open(tmpdir + "/output.txt", "r") as f:
                output = f.read()
                print(output)
            elapsed0 = time.time() - timer0
            generation_metadata = {
                "generatedBy": "dandi_lindi",
                "generatedByVersion": 11,
                "dandisetId": dandiset_id,
                "assetId": asset_id,
                "assetPath": asset['path'],
                "assetDownloadUrl": asset['download_url'],
                "assetSize": asset['size'],
                "generationTimestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "generationDuration": f"{elapsed0:.1f} seconds",
                "console_output": open(tmpdir + "/output.txt", "r").read()
            }
            with open(tmpdir + '/nwb.lindi.json', 'r') as f:
                lindi_json = json.load(f)
                lindi_json['generationMetadata'] = generation_metadata
            info = {
                'generationMetadata': lindi_json['generationMetadata'],
            }
            with open(tmpdir + '/nwb.lindi.json', 'w') as f:
                json.dump(lindi_json, f, indent=2, sort_keys=True)
            with open(tmpdir + '/info.json', 'w') as f:
                json.dump(info, f, indent=2)

            print(f"Uploading {file_key} to S3")
            _upload_file_to_s3(
                s3,
                "neurosift-lindi",
                file_key,
                tmpdir + "/nwb.lindi.json"
            )
            print(f"Uploading {info_file_key} to S3")
            _upload_file_to_s3(
                s3,
                "neurosift-lindi",
                info_file_key,
                tmpdir + "/info.json"
            )
            print(f"Time elapsed for asset {asset_id} ({num}): {elapsed0} seconds")
            print('')
            print('')
    finally:
        release_lock(lock)


def acquire_lock(asset_id):
    lock = filelock.FileLock(f'/tmp/dandi_lindi_{asset_id}.lock')
    try:
        lock.acquire(timeout=1)
    except filelock.Timeout:
        return None
    return lock


def release_lock(lock):
    lock.release()


def _remote_file_exists(url: str) -> bool:
    # use a HEAD request to check if the file exists
    headers = {  # user-agent is required for some servers
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }
    req = urllib.request.Request(url, headers=headers, method="HEAD")
    try:
        with urllib.request.urlopen(req) as response:
            return response.getcode() == 200
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        else:
            raise e


def _create_lindi_json(nwb_url: str, lindi_json_path: str):
    store = lindi.LindiH5ZarrStore.from_file(nwb_url, opts=lindi.LindiH5ZarrStoreOpts(num_dataset_chunks_threshold=5000))
    rfs = store.to_reference_file_system()
    with open(lindi_json_path, 'w') as g:
        json.dump(rfs, g, indent=2, sort_keys=True)


def fetch_all_dandisets():
    url = "https://api.dandiarchive.org/api/dandisets/?page=1&page_size=5000&ordering=-modified&draft=true&empty=false&embargoed=false"
    with urllib.request.urlopen(url) as response:
        X = json.loads(response.read())

    dandisets: List[Dandiset] = []
    for ds in X["results"]:
        pv = ds["most_recent_published_version"]
        dv = ds["draft_version"]
        dandisets.append(
            Dandiset(
                dandiset_id=ds["identifier"],
                version=pv["version"] if pv else dv["version"],
            )
        )

    return dandisets


def _upload_file_to_s3(s3, bucket, object_key, fname):
    if fname.endswith(".html"):
        content_type = "text/html"
    elif fname.endswith(".js"):
        content_type = "application/javascript"
    elif fname.endswith(".css"):
        content_type = "text/css"
    elif fname.endswith(".png"):
        content_type = "image/png"
    elif fname.endswith(".jpg"):
        content_type = "image/jpeg"
    elif fname.endswith(".svg"):
        content_type = "image/svg+xml"
    elif fname.endswith(".json"):
        content_type = "application/json"
    elif fname.endswith(".gz"):
        content_type = "application/gzip"
    else:
        content_type = None
    extra_args = {}
    if content_type is not None:
        extra_args["ContentType"] = content_type
    num_retries = 3
    while True:
        try:
            s3.upload_file(fname, bucket, object_key, ExtraArgs=extra_args)
            break
        except Exception as e:
            print(f"Error uploading {object_key} to S3: {e}")
            time.sleep(3)
            num_retries -= 1
            if num_retries == 0:
                raise


def _download_json(url: str) -> dict:
    num_retries = 3
    while True:
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
            }
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req) as response:
                return json.loads(response.read())
        except Exception as e:
            print(f"Error downloading {url}: {e}")
            time.sleep(3)
            num_retries -= 1
            if num_retries == 0:
                raise


class Dandiset(BaseModel):
    dandiset_id: str
    version: str


class write_console_output:
    def __init__(self, path):
        self.path = path

    def __enter__(self):
        self.f = open(self.path, "w")
        self.stdout = os.dup(1)
        self.stderr = os.dup(2)
        os.dup2(self.f.fileno(), 1)
        os.dup2(self.f.fileno(), 2)

    def __exit__(self, type, value, traceback):
        os.dup2(self.stdout, 1)
        os.dup2(self.stderr, 2)
        os.close(self.stdout)
        os.close(self.stderr)
        self.f.close()


if __name__ == '__main__':
    main()
