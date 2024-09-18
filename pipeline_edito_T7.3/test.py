"""This testscript demonstrates the blosc decompression error"""

import xarray as xr

url = ("https://s3.waw3-1.cloudferro.com/emodnet/bathymetry/bathymetry_"
       "2022.zarr")

# Open the Zarr file using xarray
ds = xr.open_zarr(url)

print(ds)
print("-_"*25)
subset = ds.sel(latitude=slice(37.2000000000001, 37.300000000000104),
                longitude=slice(-20, -19.9))

print(subset)
print("-_"*25)
print(subset["elevation"].values)
