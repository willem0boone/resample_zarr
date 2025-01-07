import sys
import os.path
import xarray as xr
import pandas as pd
from pathlib import Path

from resampling.plot_logs import plot_logs
from resampling.my_store import store_from_config
from resampling.plot_zarr import plot_dataset
from resampling.object_store import ObjectStore
from resampling.down_scale import down_scale_in_batches
from resampling.down_scale import down_scale_on_the_fly

my_store = store_from_config('path/to/my_config.toml')

endpoint_url=''
bucket=''
aws_access_key_id=''
aws_secret_access_key=''
aws_session_token=''

my_object_store = ObjectStore(
    endpoint_url='str',
    aws_access_key_id='str',
    aws_secret_access_key='str',
    aws_session_token='str',
    bucket='str',
)

resampler = [
    {"dimension": "latitude",
     "range": (30, 70),
     "step": 0.5,
     "invert": True
     },
    {"dimension": "longitude",
     "range": (-10, 40),
     "step": 0.5
     },
]

url = "https://s3.waw3-1.cloudferro.com/emodnet/emodnet_arco/bio_oracle/sea_water_temperature/sea_water_temperature_bio_oracle_baseline_2000_2019/climatologydecadedepthsurf.zarr"
var = "average_sea_water_temperature_biooracle_baseline"

ds = xr.open_zarr(url)
print(ds)
