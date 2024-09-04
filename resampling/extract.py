import io
import s3fs
import xarray
import requests
import xarray as xr
from typing import Tuple
from typing import Optional
import rasterio as rio
from rasterio.windows import from_bounds
from resampling.config import Config

# call config for default settings
config = Config()


def extract_public_s3_zarr(
        url: str,
        var: Optional[str] = None,
        lon_range: Optional[Tuple[float, float]] = None,
        lat_range: Optional[Tuple[float, float]] = None,
        debug: Optional[bool] = False,
) -> xr.Dataset:
    """
    Extracts a subset of data from a public S3 Zarr store within specified
    longitude and latitude ranges.

    :param url: The URL of the Zarr store.
    :param var: Optional[string], name of the variable to be extracted.
    :param lon_range: A tuple specifying the range of longitudes to extract
    (default: (-30, 70)).
    :param lat_range: A tuple specifying the range of latitudes to extract
    (default: (-15, 90)).
    :param debug: If True, enable debugging output (default: False).
    :return: A xarray Dataset containing the extracted data.
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
        aws_secret_access_key: Optional[
            str] = config.settings.aws_secret_access_key,
        aws_session_token: Optional[str] = config.settings.aws_session_token,
        bucket: Optional[str] = config.settings.bucket,

) -> xarray.Dataset:
    """
    Open a Zarr store from S3 object storage.
    :param var: Optional[string], name of the variable to be extracted.
    :param endpoint_url: str - S3 endpoint. Default is in config.
    :param aws_access_key_id: str - S3 access key. Default is in config.
    :param aws_secret_access_key: str - S3 secret key. Default is in config.
    :param aws_session_token: str - S3 session token. Default is in config.
    :param bucket: str - S3 bucket. Default is in config.
    :param name: str - Name of the Zarr store to open.
    :param lon_range: Optional[Tuple[float, float]] - A tuple specifying the
    range of longitudes to extract. Default is None.
    :param lat_range: Optional[Tuple[float, float]] - A tuple specifying the
    range of latitudes to. Default is None.
    :param var: Optional[string], name of the variable to be extracted.

    :return: xarray.Dataset - The dataset loaded from the Zarr store.
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


def check_s3_zarr_exists(
        zarr_store_path: str,
        endpoint_url: Optional[str] = config.settings.endpoint_url,
        aws_access_key_id: Optional[str] = config.settings.aws_access_key_id,
        aws_secret_access_key: Optional[str] =
        config.settings.aws_secret_access_key,
        aws_session_token: Optional[str] = config.settings.aws_session_token,
        bucket: Optional[str] = config.settings.bucket,
) -> bool:
    """
    Checks if a Zarr store exists in the specified S3 bucket.
    """
    s3 = s3fs.S3FileSystem(
        key=aws_access_key_id,
        secret=aws_secret_access_key,
        token=aws_session_token,
        client_kwargs={'endpoint_url': endpoint_url}
    )

    # Check if bucket name is provided
    if not bucket:
        raise ValueError("Bucket name must be provided.")

    # Construct the full path to the Zarr store
    full_path = f"{bucket}/{zarr_store_path}"

    # Check if the path exists
    try:
        # Using the `ls` method to check if directory exists
        if s3.exists(full_path):
            # Check if it's a directory (Zarr store)
            if s3.isdir(full_path):
                return True
            else:
                print(f"The path '{full_path}' exists but is not a directory.")
                return False
        else:
            return False
    except Exception as e:
        print(f"Error checking S3 path: {e}")
        return False


def extract_web_nc(
        url: str,
        var: Optional[str] = None,
        lon_range: Optional[Tuple[float, float]] = None,
        lat_range: Optional[Tuple[float, float]] = None,
        debug: Optional[bool] = False,
) -> xr.Dataset:
    """
    Extract data from a Bio-Oracle NetCDF file. The NetCDF file will be
    streamed into memory, data from the specified lat-lon range is read.

    :param url: string, URL of the NetCDF file from e.g. Bio-Oracle.
    :param var: Optional[string], name of the variable to be extracted.
    :param lon_range: Optional[Tuple[float, float]] - A tuple specifying the
    range of longitudes to extract. Default is None.
    :param lat_range: Optional[Tuple[float, float]] - A tuple specifying the
    range of latitudes to. Default is None.
    :param debug: Optional[bool], if True, print debugging statements.
    :return: xarray.Dataset containing the extracted data.
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

