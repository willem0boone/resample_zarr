import numpy as np
import xarray as xr
import pandas as pd
from resampling._config import Config
from resampling.load import write_zarr_s3
from resampling.transform import make_pyramid
from resampling.transform import expand_to_global_coverage
from resampling.extract import extract_private_s3_zarr


def extract_all_ds():
    datasets = pd.read_csv("datasets.csv")

    ds = xr.Dataset()

    for i, item in enumerate(datasets.itertuples()):
        my_s3_ds = extract_private_s3_zarr(
            name=f"EDITO_DUC_{item.dataset}.zarr")
        var = item.var

        for time_index in range(len(my_s3_ds.time)):
            time_value = my_s3_ds.time.values[time_index]
            if (time_value == np.datetime64('2050') or
                time_value == np.datetime64('2090') or
                time_value == np.datetime64('2010')
                ):
                time_value = pd.Timestamp(time_value).year
                name_date = f"{item.dataset}_{time_value}"

                data_slice = my_s3_ds.isel(time=time_index)
                ds[name_date] = xr.DataArray(
                    data_slice[var].values,
                    dims=("latitude", "longitude"),
                    coords={"latitude": my_s3_ds.latitude.values,
                            "longitude": my_s3_ds.longitude.values
                            }
                )
    my_s3_ds = extract_private_s3_zarr(name="EDITO_DUC_bathymetry.zarr")
    ds["elevation"] = xr.DataArray(
        my_s3_ds["elevation"].values,
        dims=("latitude", "longitude"),
    )
    return ds


if __name__ == "__main__":

    # -------------------------------------------------------------------------
    # SETTINGS
    # -------------------------------------------------------------------------
    config = Config()

    # -------------------------------------------------------------------------
    # EXTRACT
    # -------------------------------------------------------------------------
    print("-"*50)
    print('phase 1: extracting data')
    ds = extract_all_ds()
    print(ds)

    # -------------------------------------------------------------------------
    # TRANSFORM
    # -------------------------------------------------------------------------
    print("-"*50)
    print('phase 2: transforming data')

    ds = expand_to_global_coverage(ds, step_lon=0.1, step_lat=0.1)

    merged_pyramid = make_pyramid(
        ds=ds,
        pixels_per_tile=128,
        version=0.17,
        levels=6,
    )
    print(merged_pyramid)

    # -------------------------------------------------------------------------
    # LOAD
    # -------------------------------------------------------------------------
    print("-"*50)
    print('phase 3: loading data')
    write_zarr_s3(merged_pyramid, name="EDITO_DUC_pyramid.zarr")

