import numpy as np
import xarray as xr
from typing import Union


def expand_to_global_coverage(
        ds: xr.Dataset,
        step_lon: Union[float, int],
        step_lat: Union[float, int]
    ) -> xr.Dataset:
    """
    Expands a dataset to cover the global latitude and longitude range of
    -90 to 90 degrees latitude and -180 to 180 degrees longitude. The
    resolution of the expanded dataset is defined by the step sizes provided.
    Areas where the original dataset did not have data are filled with NaN
    values.

    :param ds: The original xarray.Dataset, which should have coordinates
        'longitude' and 'latitude'.
    :type ds: xr.Dataset

    :param step_lon: The resolution of the new dataset in the longitude
        dimension.
    :type step_lon: Union[float, int]

    :param step_lat: The resolution of the new dataset in the latitude
        dimension.
    :type step_lat: Union[float, int]

    :return: A xarray.Dataset that covers the global latitude and longitude
        range with the specified resolution.
    :rtype: xr.Dataset
    """
    # Create the global latitude and longitude arrays
    global_lat = np.arange(-90, 90 + step_lat, step_lat)
    global_lon = np.arange(-180, 180 + step_lon, step_lon)

    # Initialize a new data array filled with NaN values for global coverage
    global_data = {}
    for var_name in ds.data_vars:
        global_data[var_name] = xr.DataArray(
            np.full((len(global_lat), len(global_lon)), np.nan),
            dims=("latitude", "longitude"),
            coords={"latitude": global_lat, "longitude": global_lon},
        )

    # Create a new xarray dataset with global coverage
    global_ds = xr.Dataset(global_data)

    # print(f"{global_ds=}")

    # Extract the original coordinates
    original_lat = ds['latitude'].values
    original_lon = ds['longitude'].values

    # Find the index ranges for the original data within the global dataset
    lat_start_idx = np.searchsorted(global_lat, original_lat[-1])
    lat_end_idx = lat_start_idx + len(original_lat)
    lon_start_idx = np.searchsorted(global_lon, original_lon[0])
    lon_end_idx = lon_start_idx + len(original_lon)

    # Assign the original data to the corresponding location in the global
    # dataset
    for var_name in ds.data_vars:
        # print(var_name)
        original_data = ds[var_name].values
        original_data = np.flip(original_data, axis=0)
        # print(original_data)
        # print("°"*50)
        # print(lat_start_idx, lat_end_idx)
        # print(lon_start_idx, lon_end_idx)
        global_ds[var_name][lat_start_idx: lat_end_idx, lon_start_idx:lon_end_idx] = original_data
    return global_ds
