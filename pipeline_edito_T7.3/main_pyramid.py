import numpy as np
import xarray as xr
import pandas as pd
from resampling.my_store import get_my_store
from resampling.transform import make_pyramid
from resampling.transform import expand_to_global_coverage

# setup connection with object store
my_store = get_my_store()


def extract_all_ds():
    datasets = pd.read_csv("datasets.csv")

    ds = xr.Dataset()

    for i, item in enumerate(datasets.itertuples()):

        if item.dataset not in [
            "SST_baseline",
            "SST_SSP119",
            "SST_SSP245",
            "SST_SSP585",
            "SALINITY_baseline",
            "SALINITY_SSP119",
            "SALINITY_SSP245",
            "SALINITY_SSP585"
        ]:
            continue
        print(f"reading dataset {item.dataset}")

        my_s3_ds = my_store.extract_zarr(name=f"EDITO_DUC_{item.dataset}.zarr")
        var = item.var

        for time_index in range(len(my_s3_ds.time)):
            time_value = my_s3_ds.time.values[time_index]
            if (time_value == np.datetime64('2050') or
                time_value == np.datetime64('2090') or
                time_value == np.datetime64('2010')
            ):
                time_value = pd.Timestamp(time_value).year
                name_date = f"{item.dataset}_{time_value}"
                print(name_date)

                data_slice = my_s3_ds.isel(time=time_index)
                ds[name_date] = xr.DataArray(
                    data_slice[var].values,
                    dims=("latitude", "longitude"),
                    coords={"latitude": my_s3_ds.latitude.values,
                            "longitude": my_s3_ds.longitude.values
                            }
                )
    my_s3_ds = my_store.extract_zarr(name="EDITO_DUC_bathymetry.zarr")
    ds["elevation"] = xr.DataArray(
        my_s3_ds["elevation"].values,
        dims=("latitude", "longitude"),
        # coords={"latitude": my_s3_ds.latitude.values,
        #         "longitude": my_s3_ds.longitude.values}
    )
    return ds


if __name__ == "__main__":

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
    print(ds)
    print("-"*50)

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
    my_store.write_zarr(merged_pyramid, name="EDITO_DUC_pyramid.zarr")

