import numpy as np
import xarray as xr
import pandas as pd
import datetime as dt
from typing import Dict
from typing import List
from typing import Union
from tenacity import retry
from tenacity import stop_after_attempt
from tenacity import wait_exponential
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor

from resampling.load import delete_s3_zarr
from resampling.load import write_batch_s3_zarr
from resampling.load import create_empty_zarr_s3
from resampling.extract import check_s3_zarr_exists
from resampling.define_windows import define_windows


def down_scale_on_the_fly(ds, resampler):
    new_coords = {}
    slices = {}

    for spec in resampler:
        dimension = spec['dimension']
        step = spec['step']
        range_start, range_end = spec['range']
        invert = spec.get('invert', False)  # Default to False if not specified

        # Compute the new coordinates to be at the centers of the intervals
        if step > 0:
            new_coords[dimension] = np.arange(range_start + step / 2,
                                              range_end + step / 2, step)
        else:
            new_coords[dimension] = np.arange(range_start + step / 2,
                                              range_end + step / 2, step)

        if new_coords[dimension].size == 0:
            raise ValueError(
                f"Generated new coordinates for {dimension} are empty. Check the range and step values.")

        # Reverse the coordinates if invert is True
        if invert:
            new_coords[dimension] = new_coords[dimension][::-1]
            slices[dimension] = slice(range_end, range_start, -1)
        else:
            slices[dimension] = slice(range_start, range_end)

    ds_sliced = ds.sel(**slices)

    ds_reindexed = ds_sliced
    for dim in new_coords:
        if dim in ds_sliced.dims:
            if ds_sliced[dim].size > 0:
                ds_reindexed = ds_reindexed.interp({dim: new_coords[dim]},
                                                   method='nearest')

    original_order = list(ds.dims)
    ds_reindexed = ds_reindexed.transpose(*original_order)

    return ds_reindexed


def down_scale_in_batches(ds,
                          dest_zarr,
                          resampler,
                          variables,
                          batch_size,
                          workers,
                          logger=None):

    windows, indices, dimensions = define_windows(resampler, ds)

    # Check if the target Zarr store exists
    exists = check_s3_zarr_exists(dest_zarr)
    if exists:
        if logger:
            logger.info(f"{dest_zarr} already exists, it will be deleted and"
                        f" a new empyt zarr will be created")
        delete_s3_zarr(dest_zarr)

    create_empty_zarr_s3(zarr_name=dest_zarr,
                         coordinate_ranges=dimensions,
                         variables=variables)
    total_windows = len(windows)

    for variable in variables:
        for i in range(0, total_windows, batch_size):

            batch_i = int(i / batch_size)
            batch_n = int(np.ceil(total_windows / batch_size))
            if logger:
                logger.info(f">> Working on VAR {variable} - "
                            f"batch {batch_i + 1}/{batch_n}:"
                            f"windows [{i}-{i + batch_size}]/{total_windows}")

            batch_of_windows = windows[i:i + batch_size]
            batch_of_indices = indices[i:i + batch_size]

            means = _get_means_threaded(ds=ds,
                                        var=variable,
                                        windows=batch_of_windows,
                                        workers=workers,
                                        offset=i,
                                        )

            # Write the batch to the Zarr store
            write_batch_s3_zarr(zarr_store_path=dest_zarr,
                                variable_name=variable,
                                batch_values=means,
                                indexes=batch_of_indices)
        if logger:
            logger.info(f">> Finished VAR {variable}")


def _convert_to_datetime(slice_range):
    start, end = slice_range
    start_date = pd.Timestamp(dt.datetime(start, 1, 1))
    end_date = pd.Timestamp(dt.datetime(end, 1, 1))
    return start_date, end_date


def _slice_dataset(ds: xr.Dataset,
                   window:
                   List[Dict[str, Union[int, float, List[Union[int, float]]]]]
                   ) -> xr.Dataset:
    """
       Slice an xarray.Dataset based on the specified coordinate ranges for 
       each dimension.

       :param ds: The xarray.Dataset to slice.
       :type ds: xr.Dataset
       :param window: A list of dictionaries where each dictionary specifies
           the range for a dimension. Each dictionary should have:
           - The key as the dimension name.
           - The value as the range, which can be:
               - A single value (int or float).
               - A list or tuple with one value.
               - A list or tuple with two values specifying the start and end 
               of the range.
       :type window: List[
       Dict[str, Union[int, float, List[Union[int, float]]]]
       ]

       :return: The sliced xarray.Dataset.
       :rtype: xr.Dataset
       """
    dim_names = list(ds.dims)
    slices = {}

    for dim_name in dim_names:
        if dim_name in window:
            value_range = window[dim_name]
            if isinstance(value_range, (list, tuple)) and len(
                    value_range) == 1:
                start = end = value_range[0]
            elif isinstance(value_range, (int, float, np.datetime64)):
                start = end = value_range
            else:
                start, end = value_range
            slices[dim_name] = slice(start, end)

    sliced_ds = ds.sel(**slices)
    return sliced_ds


@retry(stop=stop_after_attempt(5),
       wait=wait_exponential(multiplier=1, min=4, max=10))
def _process_window(i, window, var, ds, offset):
    global_counter = i + offset
    sliced_ds = _slice_dataset(ds, window)
    if var in sliced_ds:
        values = sliced_ds[var].values
        if len(values) == 0 or np.isnan(values).all():
            mean = np.nan
        else:
            mean = np.nanmean(values)
    else:
        mean = np.nan
    return global_counter, mean


@retry(stop=stop_after_attempt(5),
       wait=wait_exponential(multiplier=1, min=4, max=10))
def _get_means_threaded(ds, var, windows, workers, offset=0):
    results = [None] * len(windows)
    with ThreadPoolExecutor(max_workers=workers) as executor:

        futures = {
            executor.submit(_process_window, i, window, var, ds, offset):
                i for i, window in enumerate(windows)
        }

        for future in as_completed(futures):
            try:
                global_counter, result = future.result()
                results[global_counter - offset] = result
            except RecursionError as e:
                print(f"RecursionError encountered: {e}")
                raise
            except Exception as e:
                print(f"Error encountered: {e}")
                raise

    means = np.array(results)
    means = np.where(np.isnan(means), np.nan, means.astype(float))

    return means


