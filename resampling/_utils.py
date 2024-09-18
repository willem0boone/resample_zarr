import numpy as np
import xarray as xr
from typing import Dict
from typing import Optional
from numpy.typing import DTypeLike


def compute_grid_area(da):
    R = 6.371e6
    SQM_PER_HA = 10000
    dϕ = np.radians((da["latitude"][1] - da["latitude"][0]).values)
    dλ = np.radians((da["longitude"][1] - da["longitude"][0]).values)
    dA = R ** 2 * np.abs(dϕ * dλ) * np.cos(np.radians(da["latitude"]))
    areacella = dA * xr.ones_like(da)

    return areacella / SQM_PER_HA


# -----------------------------------------------------------------------------
# The function "set_zarr_encoding" is part of carbonplan_data\_utils.py. This
# module uses urlpath package which imports '_PosixFlavour' from 'pathlib' and
# results in import error. The function is moved to here to avoid the
# import error.


default_fillvals = {
    "S1": "\x00",
    "i1": -127,
    "u1": 255,
    "i2": -32767,
    "u2": 65535,
    "i4": -2147483647,
    "u4": 4294967295,
    "i8": -9223372036854775806,
    "u8": 18446744073709551614,
    "f4": 9.969209968386869e36,
    "f8": 9.969209968386869e36,
}


def set_zarr_encoding(
        ds: xr.Dataset,
        codec_config: Optional[Dict] = None,
        float_dtype: Optional[DTypeLike] = None,
        int_dtype: Optional[DTypeLike] = None,
) -> xr.Dataset:
    """Set zarr encoding for each variable in the dataset
    Parameters
    ----------
    :param int_dtype:
    :param ds : xr.Dataset: Input dataset
    :param codec_config : dict, optional Dictionary of parameters to pass to
    numcodecs.get_codec, default is {'id': 'zlib', 'level': 1}
    :param float_dtype : str or dtype, optional: Dtype to cast floating point
    variables to.
    :return ds : xr.Dataset. Output dataset with updated variable encodings
    """
    import numcodecs

    ds = ds.copy()

    if codec_config is None:
        codec_config = {"id": "zlib", "level": 1}
    compressor = numcodecs.get_codec(codec_config)

    for k, da in ds.variables.items():

        # maybe cast float type
        if np.issubdtype(da.dtype, np.floating) and float_dtype is not None:
            da = da.astype(float_dtype)

        if np.issubdtype(da.dtype, np.integer) and int_dtype is not None:
            da = da.astype(int_dtype)

        # remove old encoding
        da.encoding.clear()

        # update with new encoding
        da.encoding["compressor"] = compressor
        try:
            del da.attrs["_FillValue"]
        except KeyError:
            pass
        da.encoding["_FillValue"] = default_fillvals.get(
            da.dtype.str[-2:]
        )  # TODO: handle date/time types
        ds[k] = da

    return ds


