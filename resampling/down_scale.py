import time
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

from resampling._loggers import setup_logger
from resampling._loggers import ResourceMonitor
from resampling.object_store import ObjectStore
from resampling._define_windows import _define_windows


def down_scale_on_the_fly(
    ds: xr.Dataset,
    resampler: List[Dict[str, Union[str, float, Tuple[float, float], bool]]]
) -> xr.Dataset:
    """
    Downscale an `xarray.Dataset` by resampling its dimensions based on specified parameters.

    This function performs downscaling on-the-fly by:
    * Creating new coordinates for each dimension to match the specified step sizes.
    * Slicing and interpolating the dataset based on these new coordinates.

    :param ds: The `xarray.Dataset` to downscale.
    :type ds: xarray.Dataset
    :param resampler: A list of dictionaries specifying the resampling parameters for each dimension.
        Each dictionary must include:
        * **dimension** (str): The name of the dimension to resample.
        * **step** (float): The step size for the resampling.
        * **range** (Tuple[float, float]): The range of values for the dimension as (start, end).
        * **invert** (bool, optional): Whether to invert the dimension coordinates. Defaults to False.
    :type resampler: List[Dict[str, Union[str, float, Tuple[float, float], bool]]]
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
                f"Generated new coordinates for {dimension} are empty. "
                f"Check the range and step values.")

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
    my_store: ObjectStore,
    dest_zarr: str,
    resampler: List[Dict[str, Union[str, float, Tuple[float, float], bool]]],
    variables: List[str],
    batch_size: int,
    workers: int,
    logs: Optional[bool] = True,
    over_write: Optional[bool] = True,
    start_batch: Optional[int] = None,
    end_batch: Optional[int] = None,
) -> None:
    """
    Downscale the dataset in batches and store the results in a Zarr format.

    This function processes a large dataset by splitting it into smaller windows,
    downscaling each window, and then storing the downscaled data in a Zarr store
    in batches. It utilizes threading to process the windows concurrently.

    :param ds:
        The input xarray dataset to be downscaled.
    :type ds: xr.Dataset

    :param my_store:
        An instance of ObjectStore which handles interactions with Zarr stores,
        such as checking existence, deleting, creating, and writing to the Zarr store.
    :type my_store: ObjectStore

    :param dest_zarr:
        The path or identifier of the destination Zarr store where the downscaled data
        will be saved.
    :type dest_zarr: str

    :param resampler: A list of dictionaries specifying the resampling parameters for each dimension.
        Each dictionary must include:
        * dimension (str): The name of the dimension to resample.
        * step (float): The step size for the resampling.
        * range (Tuple[float, float]): The range of values for the dimension as (start, end).
        * invert (bool, optional): Whether to invert the dimension coordinates. Defaults to False.
    :type resampler: List[Dict[str, Union[str, float, Tuple[float, float], bool]]]

    :param variables:
        A list of variable names to be processed and downscaled.
    :type variables: List[str]

    :param batch_size:
        The number of windows to process in a single batch.
    :type batch_size: int

    :param workers:
        The number of worker threads to use for parallel processing.
    :type workers: int

    :param logs:
        Whether to log progress messages. Defaults to True.
    :type logs: Optional[bool]

    :param over_write:
        Whether to overwrite an existing Zarr store. If set to True, the
        existing store will be deleted and recreated. Defaults to True.
    :type over_write: Optional[bool]

    :param start_batch:
        The starting batch index to process. If specified, the function will
        only process batches from this index onward. Defaults to None.
    :type start_batch: Optional[int]

    :param end_batch:
        The ending batch index to process. If specified, the function will stop
         processing after this batch. Defaults to None.
    :type end_batch: Optional[int]

    :return:
        None
    :rtype: None

    This function performs the following steps:

    1. It starts resource monitoring and sets up logging.
    2. It calculates the necessary windows and indices for processing the dataset.
    3. It checks if the target Zarr store exists. If it does, the store is deleted and recreated.
    4. It iteratively processes each variable by:
       * Splitting the dataset into windows of data.
       * Downscaling the data within each window using multithreading.
       * Writing the downscaled data to the Zarr store in batches.
    5. Logs the progress and completion of each batch and variable.
    """
    if start_batch is None:
        start_batch = 0  # Start from the beginning
    if end_batch is None:
        end_batch = float('inf')

    if logs:
        resource_monitor = ResourceMonitor()
        resource_monitor.start_monitor_resources()
        logger = setup_logger()

        logger.info(f"Downscaling to dataset: {dest_zarr}")

    windows, indices, dimensions = _define_windows(resampler, ds)

    # Check if the target Zarr store exists

    exists = my_store.check_zarr_exists(dest_zarr)
    if exists:
        if over_write:
            if logs:
                logger.info(
                    f"{dest_zarr} already exists, it will be deleted and"
                    f" a new empyt zarr will be created")
            my_store.delete_zarr(dest_zarr)
            my_store.create_empty_zarr(zarr_name=dest_zarr,
                                       coordinate_ranges=dimensions,
                                       variables=variables)
    else:
        my_store.create_empty_zarr(zarr_name=dest_zarr,
                                   coordinate_ranges=dimensions,
                                   variables=variables)

    total_windows = len(windows)

    for variable in variables:
        for i in range(0, total_windows, batch_size):

            batch_i = int(i / batch_size)
            # print(batch_i)

            if not start_batch <= batch_i <= end_batch:
                # print('skip')
                continue

            batch_n = int(np.ceil(total_windows / batch_size))
            if logs:
                logger.info(f">> Working on VAR {variable} - "
                            f"batch {batch_i + 1}/{batch_n}:"
                            f"windows [{i}-{i + batch_size}]/{total_windows}")

                print(f">> Working on VAR {variable} - "
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
            # means = _get_means_looped(ds=ds,
            #                           var=variable,
            #                           windows=batch_of_windows,
            #                           offset=i)
            # print("__")
            # print(means)

            # Write the batch to the Zarr store
            my_store.write_zarr_batch(
                zarr_store_path=dest_zarr,
                variable_name=variable,
                batch_values=means,
                indexes=batch_of_indices)
        if logs:
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


# @retry(stop=stop_after_attempt(5),
#        wait=wait_exponential(multiplier=1, min=4, max=10))
def _process_window(i, window, var, ds, offset, max_retries=5, retry_delay=10):
    # print(window)
    global_counter = i + offset
    retries = 0

    while retries < max_retries:
        try:
            sliced_ds = _slice_dataset(ds, window)
            # print(sliced_ds)
            if var in sliced_ds:
                # print(1)
                values = sliced_ds[var].values
                # print('-')
                if len(values) == 0 or np.isnan(values).all():
                    # print(2)
                    mean = np.nan
                else:
                    mean = np.nanmean(values)
                    # print(3)
            else:
                mean = np.nan
            # print(global_counter)
            # print(mean)
            return global_counter, mean

        except Exception as e:
            # print('going in exception')
            # print(e)
            retries += 1
            if retries >= max_retries:
                mean = np.nan
                print(f"Error in process window {window}")
                print(e)
                return global_counter, mean
            else:
                print(e)
                print(f"Error in process window {window}.")
                print(e)
                print(f"Retrying ({retries}/{max_retries})...")
                time.sleep(retry_delay)


# @retry(stop=stop_after_attempt(5),
#        wait=wait_exponential(multiplier=1, min=4, max=10))
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


def _get_means_looped(ds, var, windows, offset=0):
    results = [None] * len(windows)

    # Using a simple for loop to process each window sequentially
    for i, window in enumerate(windows):
        try:
            global_counter, result = _process_window(i, window, var, ds, offset)
            results[global_counter - offset] = result
        except RecursionError as e:
            print(f"RecursionError encountered: {e}")
            raise
        except Exception as e:
            print(f"Error encountered: {e}")
            raise

    # Convert the results to a NumPy array and handle NaN values
    means = np.array(results)
    means = np.where(np.isnan(means), np.nan, means.astype(float))

    return means
