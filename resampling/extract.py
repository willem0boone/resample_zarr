import io
import s3fs
import xarray
import requests
import datatree
import xarray as xr
import datetime as dt
import rasterio as rio
from typing import Tuple
from typing import Optional
from resampling._config import Config
from rasterio.windows import from_bounds


# call config for default settings
config = Config()


def extract_public_s3_zarr(
    url: str,
    var: Optional[str] = None,
    lon_range: Optional[Tuple[float, float]] = None,
    lat_range: Optional[Tuple[float, float]] = None,
    debug: Optional[bool] = False
) -> xr.Dataset:
    """
    Extract a subset of data from a public S3 Zarr store within specified
    longitude and latitude ranges.

    This function opens a Zarr store from a public S3 URL, optionally renames
    coordinate dimensions to standard names, subsets the dataset to the
    specified geographic ranges, and extracts a particular variable if
    specified. Debugging output can be enabled to display information about
    the data subset.

    :param url: The URL of the Zarr store from which data is to be extracted.
    :type url: str

    :param var: The name of the variable to be extracted from the dataset.
        If None, all variables are included. Defaults to None.
    :type var: Optional[str]

    :param lon_range: A tuple specifying the range of longitudes to extract,
        given as (min_lon, max_lon). Defaults to None, which means no longitude
        subsetting is performed.
    :type lon_range: Optional[Tuple[float, float]]

    :param lat_range: A tuple specifying the range of latitudes to extract,
        given as (min_lat, max_lat). Defaults to None, which means no latitude
        subsetting is performed.
    :type lat_range: Optional[Tuple[float, float]]

    :param debug: If True, enables debugging output to display the shape of
        the data subset and the dataset coordinates. Defaults to False.
    :type debug: Optional[bool]

    :return: An `xarray.Dataset` containing the extracted data subset. If a
        variable name is specified, returns the specific variable as an
        `xarray.DataArray`.
    :rtype: xr.Dataset or xr.DataArray

    :raises ValueError: If the specified variable is not found in the dataset.
    """

    # Open the Zarr store
    ds = xr.open_zarr(store=url)

    # Rename coordinates if necessary
    if 'lon' in ds.coords:
        ds = ds.rename({"lon": "longitude"})
    if 'lat' in ds.coords:
        ds = ds.rename({"lat": "latitude"})

    # Subset the dataset to the specified longitude and latitude range
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

    if debug:
        print(f"Data subset shape: {ds.shape}")
        print(f"Coordinates:\n{ds.coords}")

    return ds


def extract_private_s3_zarr(
    name: str,
    var: Optional[str] = None,
    lon_range: Optional[Tuple[float, float]] = None,
    lat_range: Optional[Tuple[float, float]] = None,
    endpoint_url: Optional[str] = config.settings.endpoint_url,
    aws_access_key_id: Optional[str] = config.settings.aws_access_key_id,
    aws_secret_access_key: Optional[str] =
    config.settings.aws_secret_access_key,
    aws_session_token: Optional[str] = config.settings.aws_session_token,
    bucket: Optional[str] = config.settings.bucket,
) -> xarray.Dataset:
    """
    Extract a Zarr dataset from a private S3 bucket.

    :param name: Name of the dataset.
    :type name: str

    :param var: Variable to extract, if specified. Default is None.
    :type var: Optional[str]

    :param lon_range: Longitude range as a tuple of (min_lon, max_lon).
        Default is None.

    :type lon_range: Optional[Tuple[float, float]]

    :param lat_range: Latitude range as a tuple of (min_lat, max_lat).
        Default is None.

    :type lat_range: Optional[Tuple[float, float]]

    :param endpoint_url: S3 endpoint URL. If not provided, defaults to the
        value in the `TOML` configuration file.

    :type endpoint_url: Optional[str]

    :param aws_access_key_id: AWS access key ID. If not provided, defaults to
        the value in the `TOML` configuration file.

    :type aws_access_key_id: Optional[str]

    :param aws_secret_access_key: AWS secret access key. If not provided,
        defaults to the value in the `TOML` configuration file.

    :type aws_secret_access_key: Optional[str]

    :param aws_session_token: AWS session token. If not provided, defaults to
        the value in the `TOML` configuration file.

    :type aws_session_token: Optional[str]

    :param bucket: S3 bucket name. If not provided, defaults to the value in
        the `TOML` configuration file.

    :type bucket: Optional[str]

    :return: The extracted dataset as an xarray Dataset.
    :rtype: xarray.Dataset
    """

    bucket = bucket + "/" + name

    s3 = s3fs.S3FileSystem(
        key=aws_access_key_id,
        secret=aws_secret_access_key,
        token=aws_session_token,
        client_kwargs={'endpoint_url': endpoint_url}
    )

    store = s3fs.S3Map(root=bucket, s3=s3, create=False)
    ds = xarray.open_zarr(store=store, consolidated=True)

    if 'lon' in ds.coords:
        ds = ds.rename({"lon": "longitude"})
    if 'lat' in ds.coords:
        ds = ds.rename({"lat": "latitude"})

    # Subset the dataset to the specified longitude and latitude range
    sel_kwargs = {}
    if lon_range is not None:
        sel_kwargs['longitude'] = slice(lon_range[0], lon_range[1])
    if lat_range is not None:
        sel_kwargs['latitude'] = slice(lat_range[0], lat_range[1])
    if sel_kwargs:
        ds = ds.sel(**sel_kwargs)

    # Select the specified variable
    if var is not None:
        if var in ds.data_vars:
            ds = ds[var]
        else:
            raise ValueError(f"Variable '{var}' not found in the dataset.")

    return ds


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
    Write a Zarr dataset to an S3 bucket.

    This function writes an `xarray.Dataset` or `datatree.DataTree` object to
    an S3 bucket as a Zarr store. It supports specifying the Zarr store name,
    mode of writing, and AWS credentials. If no name is provided, a default
    name with a timestamp will be generated.

    :param dataset: The dataset to be written to the Zarr store. Can be either
        an `xarray.Dataset` or `datatree.DataTree`.
    :type dataset: datatree.DataTree | xr.Dataset

    :param name: The name of the Zarr store to be created. If not provided,
        a default name based on the current timestamp will be used.
    :type name: Optional[str]

    :param mode: The mode for writing the Zarr store. If not provided, defaults
        to 'w' (write mode). Other modes such as 'a' (append) may be supported
        depending on the Zarr library implementation.
    :type mode: Optional[str]

    :param endpoint_url: The S3 endpoint URL. If not provided, defaults to
        the value in the `TOML` configuration file.
    :type endpoint_url: Optional[str]

    :param aws_access_key_id: The AWS access key ID. If not provided, defaults
        to the value in the `TOML` configuration file.
    :type aws_access_key_id: Optional[str]

    :param aws_secret_access_key: The AWS secret access key. If not provided,
        defaults to the value in the `TOML` configuration file.
    :type aws_secret_access_key: Optional[str]

    :param aws_session_token: The AWS session token. If not provided, defaults
        to the value in the `TOML` configuration file.
    :type aws_session_token: Optional[str]

    :param bucket: The S3 bucket name where the Zarr store will be created.
        If not provided, defaults to the value in the `TOML` configuration
        file.
    :type bucket: Optional[str]

    :return: None
    :rtype: None

    :raises ValueError: If `bucket` or `dataset` is not provided.

    :raises Exception: If there is an error during the Zarr store creation or
    writing process.

    .. note::
        The `s3fs` library is used to handle interactions with the S3 bucket.
        Ensure that `s3fs` and `zarr` libraries are installed and properly
        configured.
    """
    if mode is None:
        mode = "w"

    if not name:
        name = f"new_zarr_{dt.datetime.now().strftime('%Y-%m-%d_%H-%M')}.zarr"
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


def _extract_web_nc(
        url: str,
        var: Optional[str] = None,
        lon_range: Optional[Tuple[float, float]] = None,
        lat_range: Optional[Tuple[float, float]] = None,
        debug: Optional[bool] = False
    ) -> xr.Dataset:
    """
    Extract data from a Bio-Oracle NetCDF file. The NetCDF file will be
    streamed into memory, and data from the specified latitude and longitude
    range will be read.

    :param url: The URL of the NetCDF file, e.g., from Bio-Oracle.
    :type url: str

    :param var: The name of the variable to be extracted.
    :type var: Optional[str]

    :param lon_range: A tuple specifying the range of longitudes to extract.
        Defaults to None, meaning no longitude range is applied.
    :type lon_range: Optional[Tuple[float, float]]

    :param lat_range: A tuple specifying the range of latitudes to extract.
        Defaults to None, meaning no latitude range is applied.
    :type lat_range: Optional[Tuple[float, float]]

    :param debug: If True, print debugging statements. Defaults to False.
    :type debug: Optional[bool]

    :return: An xarray Dataset containing the extracted data.
    :rtype: xr.Dataset
    """
    # Download the file content using requests
    response = requests.get(url)
    response.raise_for_status()  # Ensure the request was successful
    data = response.content

    # Open the NetCDF dataset with xarray
    ds = xr.open_dataset(io.BytesIO(data))

    # Rename coordinates if necessary
    if 'lon' in ds.coords:
        ds = ds.rename({"lon": "longitude"})
    if 'lat' in ds.coords:
        ds = ds.rename({"lat": "latitude"})

    # Subset the dataset to the specified longitude and latitude range
    sel_kwargs = {}
    if lon_range is not None:
        sel_kwargs['longitude'] = slice(lon_range[0], lon_range[1])
    if lat_range is not None:
        sel_kwargs['latitude'] = slice(lat_range[0], lat_range[1])
    if sel_kwargs:
        ds = ds.sel(**sel_kwargs)

    # Select the specified variable
    if var is not None:
        if var in ds.data_vars:
            ds = ds[var]
        else:
            raise ValueError(f"Variable '{var}' not found in the dataset.")

    if debug:
        print(f"Data subset shape: {ds.shape}")
        print(f"Coordinates:\n{ds.coords}")

    return ds


def _extract_tif(
        tif: str,
        lon_range: Optional[Tuple[float, float]] = (-30, 70),
        lat_range: Optional[Tuple[float, float]] = (-15, 90),
        lon_step: Optional[float] = 0.5,
        lat_step: Optional[float] = 0.5,
    ):
    """
    Extracts data from a window in a geotif to an array. Extend and shape of
    the window are determined by the provided params.
    :param tif: path to a geotif file.
    :param lon_range: Tuple specifying the range of longitudes to extract,
    default is (-30, 70).
    :param lat_range: Tuple specifying the range of latitudes to extract,
    default is (-15, 90).
    :param lon_step: Longitude step size for upsampled data, default is 0.5.
    :param lat_step: Latitude step size for upsampled data, default is 0.5.
    :param debug [bool] print statements.
    :return:
    """
    # define shape
    out_shape = (1,
                 int((lat_range[1]-lat_range[0])/lat_step),
                 int((lon_range[1] - lon_range[0]) / lon_step),
                 )

    # open data
    with rio.open(tif) as src:

        # define window
        affine = src.make_pyramid
        my_window = from_bounds(left=lon_range[0],
                                right=lon_range[1],
                                bottom=lat_range[0],
                                top=lat_range[1],
                                transform=affine
                                )
        # read data
        data = src.read(1,
                        window=my_window,
                        out_shape=out_shape
                        )
        return data

