Make Pyramid
============

Implement `ndpyramid <https://github.com/carbonplan/ndpyramid/tree/main>`_
(Credit: Carbonplan) to create a pyramid datatree.datatree 'pyramid' in which
each level has a higher resolution. Such pyramids are compatible with several
Carbonplan webviewer and can be tested in
`ncview <https://ncview-js.staging.carbonplan.org/>`_.

1. Imports
----------
.. code-block:: python

    import sys
    from pathlib import Path
    import numpy as np
    import xarray as xr
    import pandas as pd
    import matplotlib.pyplot as plt

Import Resampling modules

.. code-block:: python

    parent_dir = Path().resolve().parent
    sys.path.append(str(parent_dir))
    from resampling.my_store import get_my_store
    from resampling.transform import make_pyramid
    from resampling.transform import expand_to_global_coverage

2. Load datasets
----------------

.. code-block:: python

    def extract_all_ds():
        my_dataset = xr.Dataset()

        bathy = "https://minio.lab.dive.edito.eu/oidc-willemboone/EDITO_DUC/EDITO_DUC_bathymetry.zarr"
        saly = "https://minio.lab.dive.edito.eu/oidc-willemboone/EDITO_DUC/EDITO_DUC_SALINITY_baseline.zarr"
        temp = "https://minio.lab.dive.edito.eu/oidc-willemboone/EDITO_DUC/EDITO_DUC_SST_baseline.zarr"

        ds_bathy = xr.open_zarr(bathy)
        ds_saly = xr.open_zarr(saly).sel(time="2010-01-01")
        ds_temp = xr.open_zarr(temp).sel(time="2010-01-01")

        my_dataset["saly"] = xr.DataArray(ds_saly["average_sea_water_practical_salinity_biooracle_baseline"].values,
                                     dims=("latitude", "longitude"),
                                     coords={"latitude": ds_saly.latitude.values,
                                             "longitude": ds_saly.longitude.values}
                                    )
        my_dataset["SST"] = xr.DataArray(ds_temp["average_sea_water_temperature_biooracle_baseline"].values,
                                         dims=("latitude", "longitude"),
                                         coords={"latitude": ds_temp.latitude.values,
                                                 "longitude": ds_temp.longitude.values}
                                        )
        my_dataset["elevation"] = xr.DataArray(ds_bathy["elevation"].values,
                                               dims=("latitude", "longitude"),
                                              )

        return my_dataset

.. code-block:: python

    ds = extract_all_ds()

3. Convert datasets
-------------------

Ensure global coverage
^^^^^^^^^^^^^^^^^^^^^^

Expand to global coverage, make sure the lat & lon step are conform the lat/lon
resolution of the dataset. Carbonplan maps requires the datasets to have a
global extend. Therefore the function *expand_to_global_coverage* can be used.

.. code-block:: python

    global_ds = expand_to_global_coverage(ds, step_lon=0.1, step_lat=0.1)
    print(global_ds)

Notice that matplotlib & xarray don't always understands the meaning of
latitude/longitude as geographical coordinates. On the y axis, coordinates go
from 1800 (north pole> to 900 (equator> to 0 (south pole>. Notice that this is
high to low while the plot uses low to high. so the plotting of the map is
mirrored but it doesn't affect the dataset and pyramid building.

Create pyramid
^^^^^^^^^^^^^^

.. code-block:: python

    merged_pyramid = make_pyramid(
        ds=ds,
        pixels_per_tile=128,
        version=0.17,
        levels=6,
    )

4. Extract
----------

.. code-block:: python

    my_store = get_my_store()
    my_store.write_zarr(merged_pyramid, name="tutorial_pyramid.zarr")

5. Make data public accessible
------------------------------

Run this command in a terminal in the datalab to make your dataset public
available:
.. code-block:: console

    mc anonymous set public "s3/oidc-willemboone/tutorial_pyramid.zarr"

Copy the link of your dataset:

    https://minio.lab.dive.edito.eu/oidc-willemboone/EDITO_DUC/tutorial_pyramid.zarr

And visualise is in this webviewer: https://ncview-js.staging.carbonplan.org/