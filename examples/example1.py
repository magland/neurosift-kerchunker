from fsspec.implementations.reference import ReferenceFileSystem
import zarr
from hdmf_zarr.nwb import NWBZarrIO
import pynwb
import remfile
import h5py

####################################################################################################
# EXAMPLE
# 000409
# sub-PL015/sub-PL015_ses-1d4a7bd6-296a-48b9-b20e-bd0ac80750a5_behavior+ecephys+image.nwb
# https://neurosift.app/?p=/nwb&dandisetId=000409&dandisetVersion=draft&url=https://api.dandiarchive.org/api/assets/54b277ce-2da7-4730-b86b-cfc8dbf9c6fd/download/
# https://api.dandiarchive.org/api/assets/54b277ce-2da7-4730-b86b-cfc8dbf9c6fd/download/
# https://kerchunk.neurosift.org/dandi/dandisets/000409/assets/54b277ce-2da7-4730-b86b-cfc8dbf9c6fd/zarr.json
####################################################################################################

# This one seems to load properly
# https://neurosift.app/?p=/nwb&dandisetId=000717&dandisetVersion=draft&url=https://api.dandiarchive.org/api/assets/3d12a902-139a-4c1a-8fd0-0a7faf2fb223/download/
h5_url = 'https://api.dandiarchive.org/api/assets/3d12a902-139a-4c1a-8fd0-0a7faf2fb223/download/'
json_url = 'https://kerchunk.neurosift.org/dandi/dandisets/000717/assets/3d12a902-139a-4c1a-8fd0-0a7faf2fb223/zarr.json'


# This fails with error: Could not construct ImageSeries object due to: ImageSeries.__init__: incorrect type for 'starting_frame' (got 'int', expected 'Iterable')
# https://neurosift.app/?p=/nwb&dandisetId=000409&dandisetVersion=draft&url=https://api.dandiarchive.org/api/assets/54b277ce-2da7-4730-b86b-cfc8dbf9c6fd/download/
# h5_url = 'https://api.dandiarchive.org/api/assets/54b277ce-2da7-4730-b86b-cfc8dbf9c6fd/download/'
# json_url = 'https://kerchunk.neurosift.org/dandi/dandisets/000409/assets/54b277ce-2da7-4730-b86b-cfc8dbf9c6fd/zarr.json'


def load_with_kerchunk():
    fs = ReferenceFileSystem(json_url)
    store = fs.get_mapper(root='/', check=False)
    root = zarr.open(store, mode='r')

    # Load NWB file
    with NWBZarrIO(path=root, mode="r", load_namespaces=True) as io:
        nwbfile = io.read()
        print(nwbfile)
        print('********************************')
        print(nwbfile.acquisition)


def load_with_h5_streaming():
    remf = remfile.File(h5_url)
    h5f = h5py.File(remf, mode='r')
    with pynwb.NWBHDF5IO(file=h5f, mode='r', load_namespaces=True) as io:
        nwbfile = io.read()
        print(nwbfile)
        print('********************************')
        print(nwbfile.acquisition)


if __name__ == "__main__":
    load_with_kerchunk()
    # load_with_h5_streaming()
