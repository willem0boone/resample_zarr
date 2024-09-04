import itertools
import numpy as np
import xarray as xr
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from typing import Union
from datetime import datetime


def convert_to_datetime(value: Union[int, float, datetime]) -> np.datetime64:
    """Convert a timestamp or datetime to np.datetime64."""
    if isinstance(value, datetime):
        return np.datetime64(value)
    elif isinstance(value, (int, float)):
        return np.datetime64(datetime.fromtimestamp(value))
    else:
        raise ValueError(
            f"Unsupported type for datetime conversion: {type(value)}")


def convert_to_timedelta(value: Union[int, float]) -> np.timedelta64:
    """

    Parameters
    ----------
    value

    Returns
    -------

    """
    return np.timedelta64(int(value), 'D')


def _handle_datetime_dimension(start: np.datetime64, stop: np.datetime64,
                               step: np.timedelta64, invert: bool) -> Tuple[
    List[np.datetime64], List[int]]:
    """

    Parameters
    ----------
    start
    stop
    step
    invert

    Returns
    -------

    """

    intervals = []
    indices = []
    index = 0
    current = start

    while current < stop:
        next_interval = min(current + step, stop)
        intervals.append([current, next_interval])
        indices.append(index)
        current = next_interval
        index += 1

    if invert:
        intervals = intervals[::-1]

    return intervals, indices


def _handle_numeric_dimension(start: float, stop: float, step: float,
                              invert: bool) -> Tuple[
    List[Union[float, List[float]]], List[int]]:
    """

    Parameters
    ----------
    start
    stop
    step
    invert

    Returns
    -------

    """
    intervals = []
    indices = []
    index = 0
    current = start

    if start == stop:
        intervals.append(start)
        indices.append(index)
    elif abs(stop - start) < step:
        intervals.append([start, stop])
        indices.append(index)
    else:
        while current < stop:
            next_interval = min(current + step, stop)
            intervals.append([current, next_interval])
            indices.append(index)
            current = next_interval
            index += 1

        if invert:
            intervals = intervals[::-1]

    return intervals, indices


def _process_resampler_dimension(
        dimension: Dict[str, Any]
) -> Tuple[Dict[str, Any], Dict[str, int]]:
    """

    Parameters
    ----------
    dimension

    Returns
    -------

    """

    name = dimension["dimension"]

    if isinstance(dimension["range"], (list, tuple)):

        start, stop = dimension["range"]

        step = convert_to_timedelta(dimension.get("step", 1)) \
            if isinstance(start, datetime) or isinstance(stop, datetime) \
            else float(dimension.get("step", 1))

        start = convert_to_datetime(start) \
            if isinstance(start, datetime) \
            else float(start)

        stop = convert_to_datetime(stop) \
            if isinstance(stop, datetime) \
            else float(stop)

    else:
        start = convert_to_datetime(dimension["range"]) \
            if isinstance(dimension["range"], datetime) \
            else float(dimension["range"])

        stop = start

        step = convert_to_timedelta(1) \
            if isinstance(start, np.datetime64) \
            else 1

    invert = dimension.get("invert", False)

    if isinstance(start, np.datetime64):
        intervals, indices = (
            _handle_datetime_dimension(start, stop, step, invert))
    else:
        intervals, indices = (
            _handle_numeric_dimension(start, stop, step, invert))

    return {name: intervals}, {name: indices}


def _get_missing_dimensions(ds: xr.Dataset,
                            specified_dimensions: set
                            ) -> Tuple[Dict[str, Any], Dict[str, int]]:
    """

    Parameters
    ----------
    ds
    specified_dimensions

    Returns
    -------

    """
    dimensions = {}
    dimension_indices = {}

    for dim in ds.dims.keys():
        if dim not in specified_dimensions:
            values = ds[dim].values
            if isinstance(values[0], np.datetime64):
                intervals = list(values)
                indices = list(range(len(intervals)))
            else:
                intervals = [[i] for i in range(ds.sizes[dim])]
                indices = list(range(len(intervals)))

            dimensions[dim] = intervals
            dimension_indices[dim] = indices

    return dimensions, dimension_indices


def define_windows(
        resampler: List[Dict[str, Any]],
        ds: xr.Dataset
    ) -> Tuple[
        List[Dict[str, Union[int, float, List[Union[int, float]]]]],
        List[Dict[str, int]],
        Dict[str, List[Union[int, List[Union[int, float]]]]]
    ]:
    """
    Defines the windows (intervals) for each dimension based on the provided
    resampler configuration, and includes any dimensions present in the dataset
    but not specified in the resampler.

    :param resampler: A list of dictionaries where each dictionary specifies:
        dimension (str): The name of the dimension;
        range (Union[int, Tuple[Union[int, float], Union[int, float]]]):
        The range of the dimension. It can be a single value or a tuple indicating
        the start and stop values;
        step (Optional[Union[int, float]]): The step size for creating intervals.
        Defaults to 1 if not provided;
        invert (Optional[bool]): If True, the intervals for this dimension are
        reversed. Defaults to False;

    :type resampler: List[Dict[str, Any]]

    :param ds: The xarray dataset to compare the resampler with.
    
    :type ds: xarray.Dataset

    :return: A tuple containing:
        A list of dictionaries where each dictionary represents a combination of
        intervals for each dimension;
        A list of dictionaries where each dictionary represents a combination of
        indices for each dimension;
        A dictionary where keys are dimension names and values are lists of
        intervals for each dimension;

    :rtype: Tuple[
        List[Dict[str, Union[int, float, List[Union[int, float]]]]],
        List[Dict[str, int]],
        Dict[str, List[Union[int, List[Union[int, float]]]]]
    ]
    """

    specified_dimensions = set(
        dimension["dimension"] for dimension in resampler)

    dimensions = {}
    dimension_indices = {}

    # Process specified dimensions
    for dimension in resampler:
        dim_intervals, dim_indices = _process_resampler_dimension(dimension)
        dimensions.update(dim_intervals)
        dimension_indices.update(dim_indices)

    # Process dimensions not specified in the resampler
    missing_dimensions, missing_indices = (
        _get_missing_dimensions(ds, specified_dimensions))
    dimensions.update(missing_dimensions)
    dimension_indices.update(missing_indices)

    # Generate combinations
    all_combinations = list(itertools.product(*dimensions.values()))
    index_combinations = list(itertools.product(*dimension_indices.values()))

    dims_with_coords = [
        {dim: window for dim, window in zip(dimensions.keys(), combination)}
        for combination in all_combinations
    ]

    dims_with_indices = [
        {dim: index for dim, index in
         zip(dimension_indices.keys(), combination)}
        for combination in index_combinations
    ]

    return dims_with_coords, dims_with_indices, dimensions





