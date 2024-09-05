import logging
import numpy as np
import xarray as xr
import pandas as pd
import datetime as dt
from typing import Dict
from typing import List
from typing import Union
from typing import Tuple
from typing import Optional
from tenacity import retry
from tenacity import wait_exponential
from tenacity import stop_after_attempt
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor

from resampling.load import delete_s3_zarr
from resampling.load import write_batch_s3_zarr
from resampling.load import create_empty_zarr_s3
from resampling.extract import check_s3_zarr_exists
from resampling._define_windows import define_windows


def down_scale_on_the_fly(
    ds: xr.Dataset,
    resampler: List[Dict[str, Union[str, float, Tuple[float, float], bool]]]
) -> xr.Dataset:
    """
    Downscale an `xarray.Dataset` by resampling its dimensions based on specified parameters.

    This function performs downscaling on-the-fly by:
    - Creating new coordinates for each dimension to match the specified step sizes.
    - Slicing and interpolating the dataset based on these new coordinates.

    :param ds: The `xarray.Dataset` to downscale.
    :type ds: xarray.Dataset
    :param resampler: A list of dictionaries specifying the resampling parameters for each dimension.
        Each dictionary must include:
        - **dimension** (str): The name of the dimension to resample.
        - **step** (float): The step size for the resampling.
        - **range** (Tuple[float, float]): The range of values for the dimension as (start, end).
        - **invert** (bool, optional): Whether to invert the dimension coordinates. Defaults to False.
    :type resampler: list of dicts
    :return: A downscaled `xarray.Dataset` with interpolated values on new coordinates.
    :rtype: xarray.Dataset
    :raises ValueError: If generated coordinates for a dimension are empty due to invalid range or step values.
    """
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


def down_scale_in_batches(
    ds: xr.Dataset,
    dest_zarr: str,
    resampler: List[Dict[str, Union[str, float, Tuple[float, float], bool]]],
    variables: List[str],
    batch_size: int,
    workers: int,
    logger: Optional[logging.Logger] = None
) -> None:
    """
    Downscale an `xarray.Dataset` in batches and write the results to a Zarr store on S3.

    This function performs downscaling of the dataset in batches to manage memory usage and
    improve performance. It divides the data into smaller chunks, processes each chunk,
    and writes the results to a specified Zarr store on S3.

    :param ds: The `xarray.Dataset` to downscale.
    :type ds: xarray.Dataset
    :param dest_zarr: The path to the destination Zarr store on S3 where the downscaled data will be saved.
    :type dest_zarr: str
    :param resampler: A list of dictionaries specifying the resampling parameters for each dimension.
        Each dictionary must include:
        - **dimension** (str): The name of the dimension to resample.
        - **step** (float): The step size for the resampling.
        - **range** (Tuple[float, float]): The range of values for the dimension as (start, end).
        - **invert** (bool, optional): Whether to invert the dimension coordinates. Defaults to False.
    :type resampler: list of dicts
    :param variables: A list of variable names to process within the dataset.
    :type variables: list of str
    :param batch_size: The number of windows to process in each batch.
    :type batch_size: int
    :param workers: The number of parallel workers to use for processing batches.
    :type workers: int
    :param logger: Optional logger for logging progress and status. If not provided, no logging is performed.
    :type logger: Optional[logging.Logger]

    :return: None
    :rtype: None

    :raises ValueError: If the target Zarr store already exists and cannot be deleted.
    """

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
    Slice an `xarray.Dataset` based on the specified coordinate ranges for 
    each dimension.

    :param ds: The `xarray.Dataset` to slice.
    :type ds: xarray.Dataset
    :param window: A list of dictionaries where each dictionary specifies
        the range for a dimension. Each dictionary should have:
        - The key as the dimension name.
        - The value as the range, which can be:
            - A single value (int or float).
            - A list or tuple with one value.
            - A list or tuple with two values specifying the start and end 
            of the range.
    :type window: list of dicts, where each dict contains:
        - **Dimension name** (str): The name of the dimension.
        - **Range** (Union[int, float, list of Union[int, float]]): 
            - A single value (int or float) for exact coordinates.
            - A list or tuple with one value for a single range.
            - A list or tuple with two values specifying the start and end 
              of the range.
    :return: The sliced `xarray.Dataset`.
    :rtype: xarray.Dataset
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


