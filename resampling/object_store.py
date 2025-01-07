import s3fs
import xarray
import numpy as np
import xarray as xr
import datetime as dt
from typing import List
from typing import Dict
from typing import Union
from typing import Tuple
from typing import Optional


class ObjectStore:
    """
    Manages interactions with an S3-compatible object storage system and Zarr
    datasets.

    This class allows you to configure S3 credentials, interact with Zarr
    datasets stored in an S3 bucket, and perform various operations such as
    extracting datasets and checking for the existence of Zarr stores.

    :param endpoint_url: The URL of the S3-compatible object storage endpoint.
    :type endpoint_url: str

    :param aws_access_key_id: The AWS access key for authentication.
    :type aws_access_key_id: str

    :param aws_secret_access_key: The AWS secret key for authentication.
    :type aws_secret_access_key: str

    :param aws_session_token: AWS session token for temporary credentials.
    :type aws_session_token: str

    :param bucket: The name of the S3 bucket where Zarr datasets are stored.
    :type bucket: str
    """

    def __init__(self,
                 endpoint_url: str,
                 aws_access_key_id: str,
                 aws_secret_access_key: str,
                 aws_session_token: str,
                 bucket: str):
        """
        Initializes the ObjectStore class with the provided S3 credentials and
        bucket.

        :param endpoint_url: The URL of the S3-compatible object storage
            endpoint.
        :type endpoint_url: str

        :param aws_access_key_id: The AWS access key for authentication.
        :type aws_access_key_id: str

        :param aws_secret_access_key: The AWS secret key for authentication.
        :type aws_secret_access_key: str

        :param aws_session_token: AWS session token for temporary credentials.
        :type aws_session_token: str

        :param bucket: The name of the S3 bucket where Zarr datasets are
            stored.
        :type bucket: str
        """
        self._endpoint_url = endpoint_url
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._aws_session_token = aws_session_token
        self._bucket = bucket
        self._setup_s3fs()
        self._test_connection()

    def _setup_s3fs(self):
        """
        Configures the S3FileSystem using the provided credentials.
        This method initializes the S3 connection using the S3FileSystem from
        the `s3fs` library, which allows interaction with the S3 bucket.
        """
        self._s3 = s3fs.S3FileSystem(
            key=self._aws_access_key_id,
            secret=self._aws_secret_access_key,
            token=self._aws_session_token,
            client_kwargs={'endpoint_url': self._endpoint_url}
        )

    def _test_connection(self):
        """
        Test to check S3 connection using s3fs.
        This method verifies the connection to the S3 bucket by attempting to
        list the contents of the bucket.
        """
        try:
            bucket_contents = self._s3.ls(self._bucket)

            if not bucket_contents:
                pass
                # print(
                #     f"Bucket {self._bucket} is empty, but the connection is "
                #     f"successful.")
            else:
                pass
                # print(f"Bucket {self._bucket} contains: {bucket_contents}")

            # print("S3 connection test passed.")

        except FileNotFoundError:
            raise RuntimeError(
                f"Bucket {self._bucket} does not exist or is inaccessible.")

        except Exception as e:
            raise RuntimeError(
                f"An unexpected error occurred during S3 connection test: {e}")

    def extract_zarr(
            self,
            name: str,
            var: Optional[str] = None,
            lon_range: Optional[Tuple[float, float]] = None,
            lat_range: Optional[Tuple[float, float]] = None,
    ) -> xarray.Dataset:
        """
        Extracts a Zarr dataset from the specified S3 bucket.
        Optionally, subsets the dataset based on variable, longitude, and
        latitude ranges.

        :param name: The name of the Zarr dataset within the bucket.
        :type name: str

        :param var: The variable to extract from the dataset. If None, the full
            dataset is returned.
        :type var: Optional[str]

        :param lon_range: The longitude range to subset the dataset (min, max).
            If None, no subsetting is performed.
        :type lon_range: Optional[Tuple[float, float]]

        :param lat_range: The latitude range to subset the dataset (min, max).
            If None, no subsetting is performed.
        :type lat_range: Optional[Tuple[float, float]]

        :return: The extracted and optionally subsetted xarray dataset.
        :rtype: xarray.Dataset

        :raises ValueError: If the specified variable is not found in the
            dataset.
        """
        bucket = f"{self._bucket}/{name}"
        store = s3fs.S3Map(root=bucket, s3=self._s3, create=False)
        ds = xarray.open_zarr(store=store, consolidated=True)

        if 'lon' in ds.coords:
            ds = ds.rename({"lon": "longitude"})
        if 'lat' in ds.coords:
            ds = ds.rename({"lat": "latitude"})

        sel_kwargs = {}
        if lon_range is not None:
            sel_kwargs['longitude'] = slice(lon_range[0], lon_range[1])
        if lat_range is not None:
            sel_kwargs['latitude'] = slice(lat_range[0], lat_range[1])
        if sel_kwargs:
            ds = ds.sel(**sel_kwargs)

        if var is not None:
            if var in ds.data_vars:
                ds = ds[var]
            else:
                raise ValueError(f"Variable '{var}' not found in the dataset.")

        return ds

    def check_zarr_exists(self, zarr_store_path: str) -> bool:
        """
        Checks if a Zarr store exists in the specified S3 path.

        :param zarr_store_path: The relative path to the Zarr store within the
            S3 bucket.
        :type zarr_store_path: str

        :return: True if the Zarr store exists, False otherwise.
        :rtype: bool

        :raises Exception: If an error occurs while accessing the S3 path.
        """
        full_path = f"{self._bucket}/{zarr_store_path}"

        try:
            if self._s3.exists(full_path) and self._s3.isdir(full_path):
                return True
            elif self._s3.exists(full_path):
                print(f"The path '{full_path}' exists but is not a directory.")
                return False
            else:
                return False
        except Exception as e:
            print(f"Error checking S3 path: {e}")
            return False

    def write_zarr(
            self,
            dataset: xr.DataTree | xr.Dataset,
            name: Optional[str] = None,
            mode: Optional[str] = None,
    ) -> None:
        """
        Writes a Dataset or DataTree to a Zarr store on S3.

        :param dataset: The xarray Dataset or datatree DataTree to be written
            to Zarr format.
        :type dataset: datatree.DataTree | xarray.Dataset

        :param name: The name of the Zarr store. If None, a default name with
            the current timestamp is used.
        :type name: Optional[str]

        :param mode: The mode to open the Zarr store. Default is 'w' for write.
            Other options include 'a' for append and 'r+' for read and write.
        :type mode: Optional[str]

        :return: None
        :rtype: None
        """
        if mode is None:
            mode = "w"

        if not name:
            name = \
                f"new_zarr_{dt.datetime.now().strftime('%Y-%m-%d %H-%M')}.zarr"
        bucket = self._bucket + "/" + name

        store = s3fs.S3Map(root=bucket, s3=self._s3, create=True)

        dataset.to_zarr(store=store, consolidated=True, mode=mode)

    def write_zarr_batch(
            self,
            zarr_store_path: str,
            variable_name: str,
            batch_values: np.ndarray,
            indexes: list
    ) -> None:
        """
        Writes a batch of values to a specific variable in a Zarr store on S3.

        :param zarr_store_path: The path to the Zarr store within the S3
            bucket.
        :type zarr_store_path: str

        :param variable_name: The name of the variable to which the batch
            values will be written.
        :type variable_name: str

        :param batch_values: A NumPy array of values to be written to the Zarr
            store.
        :type batch_values: np.ndarray

        :param indexes: A list of dictionaries representing the indices for
            each dimension of the variable.
        :type indexes: list

        :return: None
        :rtype: None

        :raises IndexError: If the provided indices are out of bounds for the
            specified variable.
        :raises ValueError: If the batch of values is empty or contains NaN
            values.
        """
        if np.isnan(batch_values).all():
            return

        ds = self.extract_zarr(name=zarr_store_path)
        variable = ds[variable_name]
        dim_names = variable.dims

        for value, index in zip(batch_values, indexes):
            indices = [index.get(dim) for dim in dim_names]

            if all(0 <= indices[i] < variable.shape[i] for i in
                   range(len(dim_names))):
                variable[tuple(indices)] = value
            else:
                raise IndexError(f"Index out of bounds: {index}")

        self.write_zarr(dataset=ds, name=zarr_store_path, mode="r+")

    @staticmethod
    def _create_empty_ds(
            coordinate_ranges: Dict[
                str, List[Union[int, List[int], np.datetime64]]],
            variables: List[str]
    ) -> xr.Dataset:
        """
        Creates an empty xarray Dataset with specified coordinate ranges and
        variables.

        :param coordinate_ranges: A dictionary where keys are dimension names
            and values are lists of coordinate ranges.
        :type coordinate_ranges: Dict[str, List[Union[int, List[int],
            np.datetime64]]]

        :param variables: A list of variable names to include in the Dataset.
        :type variables: List[str]

        :return: An empty xarray Dataset with the specified coordinates and
            variables.
        :rtype: xarray.Dataset
        """
        coords = {}
        dimensions = []

        for dim, ranges in coordinate_ranges.items():
            if dim == "time":
                coords[dim] = np.array(ranges, dtype='datetime64[ns]')
            elif isinstance(ranges[0], list):
                coords[dim] = [(interval[0] + interval[1]) / 2 for interval in
                               ranges if len(interval) == 2]
                coords[dim].extend(
                    [interval[0] for interval in ranges if len(interval) == 1])
            else:
                coords[dim] = np.arange(ranges[0], ranges[1])

            dimensions.append(dim)

        shape = tuple(len(coords[dim]) for dim in dimensions)
        data_vars = {var: (dimensions, np.full(shape, np.nan)) for var in
                     variables}

        return xr.Dataset(data_vars, coords=coords)

    def create_empty_zarr(
            self,
            zarr_name: str,
            coordinate_ranges: Dict[str, List[Union[int, List[int]]]],
            variables: List[str]
    ) -> xr.Dataset:
        """
        Creates an empty Zarr store with the specified coordinate ranges and
        variables on S3.

        :param zarr_name: The name of the Zarr store to create.
        :type zarr_name: str

        :param coordinate_ranges: A dictionary of coordinate ranges for each
            dimension in the Dataset.
        :type coordinate_ranges: Dict[str, List[Union[int, List[int]]]]

        :param variables: A list of variable names to include in the Dataset.
        :type variables: List[str]

        :return: The created xarray Dataset.
        :rtype: xarray.Dataset
        """
        ds = self._create_empty_ds(coordinate_ranges=coordinate_ranges,
                                   variables=variables)
        self.write_zarr(ds, zarr_name)
        return ds

    def delete_zarr(self, zarr_store_path: str) -> None:
        """
        Deletes a Zarr store from the specified S3 path.

        :param zarr_store_path: The path to the Zarr store to be deleted within
            the S3 bucket.
        :type zarr_store_path: str

        :return: None
        :rtype: None

        :raises FileNotFoundError: If the Zarr store does not exist.
        :raises Exception: If an error occurs while attempting to delete the
            Zarr store.
        """
        if not self._s3.exists(zarr_store_path):
            raise FileNotFoundError(
                f"The Zarr store '{zarr_store_path}' does not exist.")

        try:
            if self._s3.isdir(zarr_store_path):
                self._s3.rm(zarr_store_path, recursive=True)
            else:
                self._s3.rm(zarr_store_path)
        except Exception as e:
            print(f"Error deleting S3 path: {e}")
            raise

