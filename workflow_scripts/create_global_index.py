from typing import List
import time
import json
import os
import tempfile
from pydantic import BaseModel
import boto3
import urllib.request
import urllib.error
import dandi.dandiarchive as da


def main():
    create_global_index()


# thisdir = os.path.dirname(os.path.realpath(__file__))
# with open(f'{thisdir}/dandiset_ids.txt', 'r') as f:
#     dandiset_ids = f.read().splitlines()
#     dandiset_ids = [x for x in dandiset_ids if x and not x.startswith('#')]


def create_global_index():
    if os.environ.get("AWS_ACCESS_KEY_ID") is None:
        raise ValueError("AWS_ACCESS_KEY_ID not set.")
    if os.environ.get("AWS_SECRET_ACCESS_KEY") is None:
        raise ValueError("AWS_SECRET_ACCESS_KEY not set.")
    if os.environ.get("S3_ENDPOINT_URL") is None:
        raise ValueError("S3_ENDPOINT_URL not set.")

    dandisets = fetch_all_dandisets()

    all_files = []
    for dandiset_index, dandiset in enumerate(dandisets):
        print("")
        print(f"Processing {dandiset.dandiset_id} version {dandiset.version} (dandiset {dandiset_index + 1} / {len(dandisets)})")
        files = handle_dandiset(dandiset.dandiset_id, dandiset.version)
        if files:
            all_files.extend(files)
    neurodata_types = []
    with tempfile.TemporaryDirectory() as tmpdir:
        with open(tmpdir + "/global_index.json", "w") as f:
            json.dump({"files": all_files, "neurodata_types": neurodata_types}, f, indent=2, sort_keys=True)
        s3 = _get_s3_client()
        print("Uploading global_index.json to S3")
        _upload_file_to_s3(
            s3,
            "neurosift-lindi",
            "dandi/global_index.json",
            tmpdir + "/global_index.json",
        )


def _get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        region_name="auto",  # for cloudflare
    )


def handle_dandiset(
    dandiset_id: str,
    dandiset_version: str,
):
    # Create the dandi parsed url
    parsed_url = da.parse_dandi_url(f"https://dandiarchive.org/dandiset/{dandiset_id}")

    with parsed_url.navigate() as (client, dandiset, assets):
        if dandiset is None:
            print(f"Dandiset {dandiset_id} not found.")
            return

        files = []

        num_consecutive_not_nwb = 0
        num_consecutive_not_found = 0
        # important to respect the iterator so we don't pull down all the assets at once
        # and overwhelm the server
        for asset_obj in dandiset.get_assets('path'):
            if not asset_obj.path.endswith(".nwb"):
                num_consecutive_not_nwb += 1
                if num_consecutive_not_nwb >= 20:
                    # For example, this is important for 000026 because there are so many non-nwb assets
                    print("Skipping dandiset because too many consecutive non-NWB files.")
                    break
                continue
            else:
                num_consecutive_not_nwb = 0
            if num_consecutive_not_found >= 20:
                print("Skipping dandiset because too many consecutive missing files.")
                break
            asset_id = asset_obj.identifier
            asset_path = asset_obj.path
            file_key = f'dandi/dandisets/{dandiset_id}/assets/{asset_id}/zarr.json'
            zarr_json_url = f'https://lindi.neurosift.org/{file_key}'
            zarr_json = _download_json(zarr_json_url)
            if not _remote_file_exists(zarr_json_url):
                num_consecutive_not_found += 1
                continue
            generation_metadata = zarr_json.get("generationMetadata", {})
            generation_version = generation_metadata.get("generatedByVersion")
            if generation_version != 9:
                num_consecutive_not_found += 1
                continue
            neurodata_types = _get_neurodata_types_for_zarr_json(zarr_json)
            file = {
                'dandiset_id': dandiset_id,
                'dandiset_version': dandiset_version,
                'asset_id': asset_id,
                'asset_path': asset_path,
                'zarr_json_url': zarr_json_url,
                'neurodata_types': neurodata_types
            }
            files.append(file)
        return files


def _get_neurodata_types_for_zarr_json(zarr_json: dict) -> List[str]:
    neurodata_types = set()
    refs = zarr_json.get("refs", {})
    for key in refs:
        if key.endswith('.zattrs'):
            zattrs = refs[key]
            ndt = zattrs.get("neurodata_type")
            if ndt:
                neurodata_types.add(ndt)
    return sorted(neurodata_types)


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


if __name__ == '__main__':
    main()
