import os
import s3fs
import datatree
import numpy as np
import xarray as xr
import datetime as dt
from typing import Dict
from typing import List
from typing import Union
from typing import Optional
from resampling.config import Config
from resampling.extract import extract_private_s3_zarr

# call config for default settings
config = Config()


def write_zarr_local(merged_pyramid: datatree.DataTree,
                     name: Optional[str] = None,
                     store: Optional[str] = None,
                     ) -> None:
    """
    Load a dataset to a storage component.

    :param merged_pyramid: xarray datatree
    :param name: string name for export.zarr.
    :param store: path to storage, e.g. c://documents. Is optional, default is
    storage in project directory.
    :return: No return.
    """
    if not name:
        name = f"pyramid_edito_{dt.datetime.now().strftime("%Y-%m-%d %H-%M")
                                }.zarr"

    if store and os.path.exists(store):
        mapper = os.path.join(store, name)

    else:
        mapper = name

    merged_pyramid.to_zarr(
        mapper,
        mode="w",
        consolidated=True,
    )


def write_zarr_s3(
        dataset: datatree.DataTree | xr.Dataset,
        name: Optional[str] = None,
        mode: Optional[str] = None,
        endpoint_url: Optional[str] = config.settings.endpoint_url,
        aws_access_key_id: Optional[str] = config.settings.aws_access_key_id,
        aws_secret_access_key: Optional[str] =
        config.settings.aws_secret_access_key,
        aws_session_token: Optional[str] = config.settings.aws_session_token,
        bucket: Optional[str] = config.settings.bucket,
        ) -> None:

    """
    Load pyramid to object storage.

    :param mode: Optional[str], mode of zarr writing.
    :param dataset: datatree.DataTree | xarray.Dataset
    :param endpoint_url: S3 endpoint. Default is in config.
    :param aws_access_key_id: S3 access key. Default is in config.
    :param aws_secret_access_key: S3 secret key. Default is in config.
    :param aws_session_token: S3 session token. Default is in config.
    :param bucket: S3 bucket. Default is in config.
    :param name: name of zarr to store.zarr
    :return: None
    """
    if mode is None:
        mode = "w"

    if not name:
        name = f"new_zarr_{dt.datetime.now().strftime("%Y-%m-%d %H-%M")}.zarr"
    bucket = bucket + "/" + name

    s3 = s3fs.S3FileSystem(
        key=aws_access_key_id,
        secret=aws_secret_access_key,
        token=aws_session_token,
        client_kwargs={'endpoint_url': endpoint_url}
    )

    store = s3fs.S3Map(root=bucket, s3=s3, create=True)

    dataset.to_zarr(store=store,
                    consolidated=True,
                    mode=mode
                    )


def write_batch_local_zarr(zarr_store_path: str,
                           variable_name: str,
                           batch_values: np.ndarray,
                           indexes: list
                           ) -> None:
    """
    Update a batch of data in a Zarr store at the specified indices.

    :param zarr_store_path: Path to the Zarr store.
    :param variable_name: Name of the variable to update.
    :param batch_values: Array of values to write.
    :param indexes: List of dictionaries containing the indices to update.
    """
    # Check for empty batch
    if np.isnan(batch_values).all():
        return

    ds = xr.open_zarr(zarr_store_path)
    variable = ds[variable_name]
    dim_names = variable.dims

    # Write the batch of values to the specified indexes
    for value, index in zip(batch_values, indexes):
        indices = [index.get(dim) for dim in dim_names]

        # Ensure the indices are within the bounds
        if all(0 <= indices[i] < variable.shape[i]
               for i in range(len(dim_names))):
            variable[tuple(indices)] = value
        else:
            raise IndexError(f"Index out of bounds: {index}")

    # Save the changes back to the Zarr store
    ds.to_zarr(zarr_store_path, mode='r+', consolidated=True)


def write_batch_s3_zarr(zarr_store_path: str,
                        variable_name: str,
                        batch_values: np.ndarray,
                        indexes: list
                        ) -> None:
    """
    Update a batch of data in a Zarr store located on S3 at the specified
    indices.

    :param zarr_store_path: Path to the Zarr store on S3.
    :type zarr_store_path: str
    :param variable_name: Name of the variable to update.
    :type variable_name: str
    :param batch_values: Array of values to write.
    :type batch_values: np.ndarray
    :param indexes: List of dictionaries containing the indices to update.
    :type indexes: list
    """
    # Check for empty batch
    if np.isnan(batch_values).all():
        return

    ds = extract_private_s3_zarr(name=zarr_store_path)
    variable = ds[variable_name]
    dim_names = variable.dims

    # Write the batch of values to the specified indexes
    for value, index in zip(batch_values, indexes):
        indices = [index.get(dim) for dim in dim_names]

        # Ensure the indices are within the bounds
        if all(0 <= indices[i] < variable.shape[i]
               for i in range(len(dim_names))):
            variable[tuple(indices)] = value
        else:
            raise IndexError(f"Index out of bounds: {index}")

    # Save the changes back to the Zarr store
    write_zarr_s3(dataset=ds, name=zarr_store_path, mode="r+")


def _create_empty_ds(
        coordinate_ranges:
        Dict[str, List[Union[int, List[int], np.datetime64]]],
        variables: List[str]
    ) -> xr.Dataset:

    coords = {}
    dimensions = []

    for dim, ranges in coordinate_ranges.items():
        if dim == "time":
            coords[dim] = np.array(ranges, dtype='datetime64[ns]')
        elif isinstance(ranges[0], list):
            coords[dim] = [(interval[0] + interval[1]) / 2
                           for interval in ranges if len(interval) == 2]
            coords[dim].extend([interval[0]
                                for interval in ranges if len(interval) == 1])
        else:
            if len(ranges) == 1:
                coords[dim] = ranges
            else:
                coords[dim] = np.arange(ranges[0], ranges[1])

        dimensions.append(dim)

    shape = tuple(len(coords[dim]) for dim in dimensions)
    data_vars = {var: (dimensions, np.full(shape, np.nan))
                 for var in variables}

    ds = xr.Dataset(data_vars, coords=coords)
    return ds


def create_empty_zarr_local(
        zarr_name: str,
        coordinate_ranges: Dict[str, List[Union[int, List[int]]]],
        variables: List[str]
    ) -> xr.Dataset:
    """
    Create an empty Zarr dataset with the specified coordinate ranges and
    variables.

    :param zarr_name: The name of the Zarr store to create.
    :type zarr_name: str

    :param coordinate_ranges: A dictionary where keys are dimension names and
        values specify the ranges for each dimension. The ranges can be:
        - A list of two integers specifying the start and stop values.
        - A list of intervals, where each interval is a list of two integers.
        - A single integer specifying the size of the dimension.
    :type coordinate_ranges: Dict[str, List[Union[int, List[int]]]]

    :param variables: A list of variable names to include in the dataset.
    :type variables: List[str]

    :return: The created xarray.Dataset.
    :rtype: xr.Dataset
    """
    ds = _create_empty_ds(coordinate_ranges=coordinate_ranges,
                          variables=variables,)

    ds.to_zarr(store=zarr_name, consolidated=True, mode="w")

    return ds


def create_empty_zarr_s3(
        zarr_name: str,
        coordinate_ranges: Dict[str, List[Union[int, List[int]]]],
        variables: List[str]
    ) -> xr.Dataset:
    """
    Create an empty Zarr dataset with the specified coordinate ranges and
    variables, to be stored in an S3 bucket.

    :param zarr_name: The name of the Zarr store to create.
    :type zarr_name: str

    :param coordinate_ranges: A dictionary where keys are dimension names and
        values specify the ranges for each dimension. The ranges can be:
        - A list of two integers specifying the start and stop values.
        - A list of intervals, where each interval is a list of two integers.
        - A single integer specifying the size of the dimension.
    :type coordinate_ranges: Dict[str, List[Union[int, List[int]]]]

    :param variables: A list of variable names to include in the dataset.
    :type variables: List[str]

    :return: The created xarray.Dataset.
    :rtype: xr.Dataset
    """
    ds = _create_empty_ds(coordinate_ranges=coordinate_ranges,
                          variables=variables,)

    write_zarr_s3(ds, zarr_name)

    return ds


def delete_s3_zarr(
        zarr_store_path: str,
        endpoint_url: Optional[str] = config.settings.endpoint_url,
        aws_access_key_id: Optional[str] = config.settings.aws_access_key_id,
        aws_secret_access_key: Optional[str] = config.settings.aws_secret_access_key,
        aws_session_token: Optional[str] = config.settings.aws_session_token,
        bucket: Optional[str] = config.settings.bucket
    ) -> None:
    """
    Deletes a Zarr file or directory from an S3 bucket.

    :param zarr_store_path: The path to the Zarr file or directory within the
        bucket.
    :type zarr_store_path: str

    :param endpoint_url: Optional; the endpoint URL for the S3 service.
    :type endpoint_url: Optional[str]

    :param aws_access_key_id: Optional; AWS access key ID for authentication.
    :type aws_access_key_id: Optional[str]

    :param aws_secret_access_key: Optional; AWS secret access key for authentication.
    :type aws_secret_access_key: Optional[str]

    :param aws_session_token: Optional; AWS session token for temporary credentials.
    :type aws_session_token: Optional[str]

    :param bucket: Optional; S3 bucket name. If not provided, it should be part of
        the `zarr_store_path`.
    :type bucket: Optional[str]

    :return: None
    :rtype: None

    :raises ValueError: If `bucket` is not provided and is not part of `zarr_store_path`.
    :raises FileNotFoundError: If the Zarr store does not exist in the bucket.
    :raises Exception: For other errors that occur during the deletion process.
    """
    # Initialize S3FileSystem with the provided credentials and endpoint URL
    s3 = s3fs.S3FileSystem(
        key=aws_access_key_id,
        secret=aws_secret_access_key,
        token=aws_session_token,
        client_kwargs={'endpoint_url': endpoint_url}
    )

    # Check if bucket name is provided
    if not bucket and not zarr_store_path.startswith("s3://"):
        raise ValueError(
            "Bucket name must be provided or be part of zarr_store_path.")

    # Construct the full path if bucket is provided
    if bucket:
        full_path = f"{bucket}/{zarr_store_path}"
    else:
        full_path = zarr_store_path

    # Check if the path exists
    if not s3.exists(full_path):
        raise FileNotFoundError(
            f"The Zarr store '{full_path}' does not exist.")

    # Delete the Zarr store
    try:
        # If the path is a directory, delete it recursively
        if s3.isdir(full_path):
            s3.rm(full_path, recursive=True)
        else:
            # If it's a single file, delete it
            s3.rm(full_path)
        # print(f"Successfully deleted '{full_path}'.")
    except Exception as e:
        print(f"Error deleting S3 path: {e}")
        raise

