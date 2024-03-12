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
import remfile
import h5py
import kerchunk.hdf  # type: ignore


def main():
    dandi_kerchunk(
        max_time_sec=60 * 120,
        max_time_sec_per_dandiset=30
    )

assets_to_skip = [ # these take too long
    # 000717
    'a6951f6e-b67e-4df3-a14f-07b7854b821c'
]


thisdir = os.path.dirname(os.path.realpath(__file__))
with open(f'{thisdir}/dandiset_ids.txt', 'r') as f:
    dandiset_ids = f.read().splitlines()
    dandiset_ids = [x for x in dandiset_ids if x]


def dandi_kerchunk(
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

    timer = time.time()
    # for dandiset in dandisets:
    for dandiset_id in dandiset_ids:
        dandiset = next((x for x in dandisets if x.dandiset_id == dandiset_id), None)
        if dandiset is None:
            print(f"Dandiset {dandiset_id} not found.")
            continue
        print("")
        print(f"Processing {dandiset.dandiset_id} version {dandiset.version}")
        kerchunk_dandiset(dandiset.dandiset_id, max_time_sec_per_dandiset)
        elapsed_sec = time.time() - timer
        print(f"Time elapsed: {elapsed_sec} seconds")
        if elapsed_sec > max_time_sec:
            print("Time limit reached.")
            break


def kerchunk_dandiset(
    dandiset_id: str,
    max_time_sec: float
):
    timer = time.time()

    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        region_name="auto",  # for cloudflare
    )

    # Create the dandi parsed url
    parsed_url = da.parse_dandi_url(f"https://dandiarchive.org/dandiset/{dandiset_id}")

    with parsed_url.navigate() as (client, dandiset, assets):
        if dandiset is None:
            print(f"Dandiset {dandiset_id} not found.")
            return

        asset_num = 0
        # Loop through all assets in the dandiset
        for asset in dandiset.get_assets():
            asset_num += 1
            if asset.path.endswith(".nwb"):  # only process NWB files
                asset_id = asset.identifier
                if asset_id in assets_to_skip:
                    print(f"Skipping {asset_id} because it is in assets_to_skip.")
                    continue
                file_key = f'dandi/dandisets/{dandiset_id}/assets/{asset_id}/zarr.json'
                info_file_key = f'dandi/dandisets/{dandiset_id}/assets/{asset_id}/info.json'
                zarr_json_url = f'https://kerchunk.neurosift.org/{file_key}'
                info_url = f'https://kerchunk.neurosift.org/{info_file_key}'
                if _remote_file_exists(zarr_json_url) and _remote_file_exists(info_url):
                    print(f"Skipping {asset_id} because it already exists.")
                    continue
                with tempfile.TemporaryDirectory() as tmpdir:
                    print(f"Processing asset {asset_id}")
                    timer0 = time.time()
                    _create_zarr_json(asset.download_url, tmpdir + "/zarr.json")
                    elapsed0 = time.time() - timer0
                    print(f"Time elapsed for this asset: {elapsed0} seconds")
                    print(f"Uploading {file_key} to S3")
                    _upload_file_to_s3(s3, "neurosift-kerchunk", file_key, tmpdir + "/zarr.json")
                    with open(tmpdir + "/info.json", "w") as f:
                        json.dump(
                            {
                                "version": 1,
                                "dandiset_id": dandiset_id,
                                "asset_id": asset_id,
                                "asset_num": asset_num,
                                "asset_path": asset.path,
                                "asset_download_url": asset.download_url,
                                "asset_size": asset.size,
                                "elapsed_sec": elapsed0,
                                "timestamp": time.time()
                            },
                            f,
                            indent=2,
                        )
                    _upload_file_to_s3(s3, "neurosift-kerchunk", info_file_key, tmpdir + "/info.json")
                    print('.')
            if time.time() - timer > max_time_sec:
                print("Time limit reached for this dandiset.")
                break


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


def _create_zarr_json(nwb_url: str, zarr_json_path: str):
    remf = remfile.File(nwb_url, verbose=False)
    with h5py.File(remf, 'r') as f:
        grp = f
        h5chunks = kerchunk.hdf.SingleHdf5ToZarr(
            grp,
            url=nwb_url,
            hdmf_mode=True,
            num_chunks_per_dataset_threshold=2
        )
        a = h5chunks.translate()
        with open(zarr_json_path, 'w') as g:
            json.dump(a, g, indent=2)


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
    s3.upload_file(fname, bucket, object_key, ExtraArgs=extra_args)


class Dandiset(BaseModel):
    dandiset_id: str
    version: str


if __name__ == '__main__':
    main()
