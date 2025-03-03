import time
import urllib.request
import urllib.error
import json
from typing import List
import os
import tempfile
from pydantic import BaseModel
import boto3
import dandi.dandiarchive as da
import lindi
from get_nwb_usage_string import get_nwb_usage_string
import pynwb
from NWBTyped import NWBTyped

max_time_sec_per_dandiset = 20
version = 'v5' # increment this to force reprocessing of all assets

def create_nwb_usage_scripts():
    dandisets = fetch_all_dandisets()

    for dandiset_index, dandiset in enumerate(dandisets):
        print("")
        print(f'Processing dandiset {dandiset_index + 1}/{len(dandisets)}: {dandiset.dandiset_id} version {dandiset.version}')
        try:
            process_dandiset(dandiset)
        except Exception as e:
            print(f'Error processing dandiset {dandiset.dandiset_id} version {dandiset.version}: {e}')


class Dandiset(BaseModel):
    dandiset_id: str
    version: str

def process_dandiset(dandiset0: Dandiset):
    dandiset_id = dandiset0.dandiset_id
    timer = time.time()

    parsed_url = da.parse_dandi_url(f"https://dandiarchive.org/dandiset/{dandiset_id}")
    with parsed_url.navigate() as (client, dandiset, assets):
        if dandiset is None:
            print(f"Dandiset {dandiset_id} not found.")
            return

    asset_index = 0
    num_assets_processed = 0
    for asset_obj in dandiset.get_assets_by_glob('*.nwb', 'path'):
        asset = {
            "identifier": asset_obj.identifier,
            "path": asset_obj.path,
            "size": asset_obj.size,
            "download_url": asset_obj.download_url,
            "dandiset_id": dandiset_id,
        }
        process_asset(asset, num=asset_index)
        asset_index += 1
        num_assets_processed += 1
        elapsed_sec = time.time() - timer
        if elapsed_sec > max_time_sec_per_dandiset:
            print(f"Time limit reached for dandiset {dandiset_id}.")
            break
    elapsed_sec = time.time() - timer
    print(f"Processed {num_assets_processed} assets in dandiset {dandiset_id} in {elapsed_sec:.2f} seconds.")


def process_asset(asset, *, num: int):
    print(f'Processing asset {num + 1}: {asset["path"]}')
    dandiset_id = asset["dandiset_id"]
    asset_id = asset["identifier"]
    path = asset["path"]
    file_key = f'dandi/dandisets/{dandiset_id}/assets/{asset_id}/nwb.lindi.json'
    file_key_usage = f'dandi/dandisets/{dandiset_id}/assets/{asset_id}/usage.py'
    usage_url = f'https://lindi.neurosift.org/{file_key_usage}'
    try:
        txt = _download_text(usage_url, retry=False)
    except:
        txt = None
    if txt is not None:
        lines = txt.split('\n')
        if len(lines) > 0 and lines[0].startswith(f'# get_nwb_usage_string {version}.'):
            print(f"Usage script already exists for asset {path}.")
            return
    print(f"Processing asset {path}...")
    lindi_json_url = f'https://lindi.neurosift.org/{file_key}'
    lindi_json = _download_json(lindi_json_url)
    if lindi_json is None:
        print(f"Failed to download Lindi JSON for asset {path} from {lindi_json_url}.")
        return
    with tempfile.TemporaryDirectory() as tmpdir:
        fname = os.path.join(tmpdir, 'nwb.lindi.json')
        with open(fname, 'w') as f:
            json.dump(lindi_json, f)
        f = lindi.LindiH5pyFile.from_lindi_file(fname)
        nwb = pynwb.NWBHDF5IO(file=f, mode='r').read()
        nwb = NWBTyped(nwb)
        usage_script = get_nwb_usage_string(nwb)
        usage_script = f'# get_nwb_usage_string {version}.' + '\n' + usage_script
        fname_usage_py = f'{tmpdir}/usage.py'
        with open(fname_usage_py, 'w') as f:
            f.write(usage_script)
        _upload_file(fname_usage_py, 'neurosift-lindi', file_key_usage)
        print(f"Uploaded usage script for asset {path} to {usage_url}.")
    print(f"Processed asset {path}.")


def _download_json(url: str) -> dict:
    num_tries = 3
    while True:
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
            }
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req) as response:
                return json.loads(response.read())
        except Exception as e:
            num_tries -= 1
            if num_tries == 0:
                raise
            time.sleep(3)


def _download_text(url: str, *, retry: bool = True) -> str:
    num_tries = 3
    if not retry:
        num_tries = 1
    while True:
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
            }
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req) as response:
                return response.read().decode('utf-8')
        except Exception as e:
            num_tries -= 1
            if num_tries == 0:
                raise
            time.sleep(3)


def _upload_file(fname, bucket, file_key):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        region_name="auto",  # for cloudflare
    )
    _upload_file_to_s3(
        s3,
        bucket,
        file_key,
        fname,
    )


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


if __name__ == "__main__":
    create_nwb_usage_scripts()
