import xarray
import datatree
import numpy as np
import xarray as xr
from ndpyramid import pyramid_reproject
from carbonplan_data.metadata import get_cf_global_attrs

from resampling.utils import set_zarr_encoding
from resampling.utils import compute_grid_area


def combine_datasets(datasets):
    """
    Combine multiple xarray.Dataset objects into a single dataset.
    :param datasets: (list of xarray.Dataset): List of datasets to combine.
    :return combined_ds (xarray.Dataset): Combined dataset.
    """
    # Merge the combined dataset along overlapping coordinates
    combined_ds = xr.merge(datasets, join='exact')

    return combined_ds


def expand_to_global_coverage(ds: xr.Dataset,
                              step_lon: float | int,
                              step_lat: float | int
                              ) -> xr.Dataset:
    """
    Expand a dataset to have global coverage lat -90 to 90 and lon -180 to 180.
    The resolution is defined by the steps given as input. The output contains
    the original dataset completed with nan values for the lat/lon ranges where
    no data was available.
    :param ds: xr.Dataset, original dataset. should have coordinate 'longitude'
     and 'latitude'.
    :param step_lon: int or float: resolution of new ds.
    :param step_lat: int or float: resolution of new ds.
    :return:  xr.Dataset.
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

    print(f"{global_ds=}")

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
        original_data = ds[var_name].values
        original_data = np.flip(original_data, axis=0)
        global_ds[var_name][lat_start_idx: lat_end_idx,
        lon_start_idx:lon_end_idx] = original_data
    return global_ds


def make_pyramid(ds, pixels_per_tile, version, levels) -> datatree.DataTree:
    """
    Transform xarray dataset into datatree pyramid ready to be used in
    carbonplan smart viewer.
    :param ds: Xarray.Dataset
    :param pixels_per_tile:
    :param version: will be stored as output dataset parameter
    :param levels: int, number of zoomlevels in the pyramid.
    :return: xarray datatree.
    """
    def _merge_layers(ds: xarray.Dataset, pixels_per_tile: int):
        da = ds.to_array(
            dim="variable").chunk(
            dict(
                x=pixels_per_tile,
                y=pixels_per_tile
            )
        )
        merged_ds = da.to_dataset(name="all_variables")
        return merged_ds

    var = list(ds.data_vars.keys())[0]
    ds["area"] = compute_grid_area(ds[var])

    ds = ds.rio.write_crs("EPSG:4326")

    print("start reproject")
    pyramid = pyramid_reproject(ds, levels=levels)

    for child in pyramid.children:
        child.ds = child.ds[list(ds.data_vars)]

    merged_pyramid = datatree.DataTree()
    merged_pyramid.ds = xr.Dataset(
        attrs=get_cf_global_attrs(version=version))


    for child in pyramid.children:
        ds = _merge_layers(child.ds, pixels_per_tile)

        ds['x'] = ds['x'].astype(np.float32)
        ds['y'] = ds['y'].astype(np.float32)

        ds['variable'] = ds['variable'].astype('<U50')
        ds['all_variables'] = ds['all_variables'].astype(np.float32)



        merged_pyramid[child.name] = set_zarr_encoding(
            ds, codec_config={"id": "zlib", "level": 1}, float_dtype="float32"
        )

    merged_pyramid.ds.attrs["multiscales"] = pyramid.ds.attrs["multiscales"]
    for level in range(len(merged_pyramid.children)):
        merged_pyramid.ds.attrs["multiscales"][0]["datasets"][level][
            "pixels_per_tile"] = pixels_per_tile

    # to replace invalid values by custom nan value
    # for child in dataset.children:
    #     for var in child.ds.data_vars:
    #         child.ds[var] = child.ds[var].where(child.ds[var] < 999999)

    return merged_pyramid


