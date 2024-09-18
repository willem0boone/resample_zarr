Resampling ZARR datasets
========================

This notebook gives a demonstration of how to resample zarr datasets.

1. Import packages
^^^^^^^^^^^^^^^^^^

Import standard packages
""""""""""""""""""""""""

.. code-block:: python

    import sys
    import os.path
    import xarray as xr
    import pandas as pd
    from pathlib import Path

Import Resampling modules
"""""""""""""""""""""""""

Before running this code, make sure your S3 credentials are stored in the
config.toml.

.. code-block:: python

    from resampling.plot_logs import plot_logs
    from resampling.my_store import get_my_store
    from resampling.plot_zarr import plot_dataset
    from resampling.down_scale import down_scale_in_batches
    from resampling.down_scale import down_scale_on_the_fly

2. Settings
^^^^^^^^^^^

Initiate ObjectStore
""""""""""""""""""""

For IO with your S3 storage, the :class:`resampling.object_store.ObjectStore`
class is used. The initiation of this class requires your S3 credentials.

You can configure this package by storing your S3 credentials in a config file.
Doing so, you can use :func:`resampling.my_store.get_my_store` to  create an
instance of :class:`resampling.object_store.ObjectStore`.

:func:`resampling.my_store.get_my_store` will look by default at a
config file stored at:

    resampling/config/config.toml

.. code-block:: python

    my_store = get_my_store()

However, you can also provide your own config.toml file.

.. code-block:: python

    my_store = get_my_store('path/to/my_config.toml')


The content of the file should look like this:

.. code-block:: python

    endpoint_url=''
    bucket=''
    aws_access_key_id=''
    aws_secret_access_key=''
    aws_session_token=''


Alternatively, you can initiate :class:`resampling.object_store.ObjectStore`
manually as follows:

.. code-block:: python

    my_object_store = ObjectStore(
        endpoint_url='str',
        aws_access_key_id='str',
        aws_secret_access_key='str',
        aws_session_token='str',
        bucket='str',
    )

Target resolution
"""""""""""""""""

Define the dimesion, ranges and resolution (step) to which the dataset should
be resampled. <br>
Note that dimensions that are in the dataset but that are not mentioned in
resampler will be maintained to the original resolution.

.. code-block:: python

    resampler = [
        {"dimension": "latitude",
         "range": (30, 70),
         "step": 0.5,
         "invert": True
         },
        {"dimension": "longitude",
         "range": (-10, 40),
         "step": 0.5
         },
    ]


3. Resample on the fly
^^^^^^^^^^^^^^^^^^^^^^

If a dataset is manageable to be read in momory without consuming to much RAM, it can be resampled in on the fly.

Define target dataset
"""""""""""""""""""""

.. code-block:: python

    url = "https://s3.waw3-1.cloudferro.com/emodnet/emodnet_arco/bio_oracle/sea_water_temperature/sea_water_temperature_bio_oracle_baseline_2000_2019/climatologydecadedepthsurf.zarr"
    var = "average_sea_water_temperature_biooracle_baseline"

Open the dataset with xarray

.. code-block:: python

    ds = xr.open_zarr(url)
    print(ds)

Rescale dataset
"""""""""""""""

Rescale the xarray dataset according the information from the resampler.

.. code-block:: python

    ds_downscaled = down_scale_on_the_fly(
        ds=ds,
        resampler=resampler
    )

.. code-block:: python

    print(ds_downscaled)

Save the result to a new zarr.

.. code-block:: python

    ds_name = "tutorial_resampled_SST_baseline"
    load.write_zarr_s3(dataset=ds_downscaled,
                       name=f"{ds_name}.zarr")

Resample using batches
^^^^^^^^^^^^^^^^^^^^^^

When reading datasets that consume more RAM then you have available, it is not feasible to process them in memory. Instead, the dataset will be devided in batches (each containing #x windows) and each window will be processed seperate.

Define target dataset
""""""""""""""""""""""

.. code-block:: python

    url = ("https://s3.waw3-1.cloudferro.com/emodnet/bathymetry/bathymetry_"
               "2022.zarr")
    var = ["elevation"]

Open the dataset with xarray, notice that this ds consumes 66GB.

.. code-block:: python

    ds = xr.open_zarr(url)
    print(ds)

Batch processing settings
^^^^^^^^^^^^^^^^^^^^^^^^^

Additional settings to regulate the window processing:

* **workers**: number of windows that will be processed in parallel. Each window will start a thread. Notice that the total number of thread will be larger than number of workers due to a number of background threads (especially since zarr is using Dask which will also open several threads).
* **batch_size**: number of windows to be processed before writing the results to the zarr. Small batch size will slow down the process because of the increasing amount of write operations. Large batch sizes could cause RAM errors.

.. code-block:: python

    params = {"resampler": resampler,
              "workers": 50,
              "batch_size": 500
             }

Process dataset
^^^^^^^^^^^^^^^

Attention! This step will take some time.<br>
You can track progress in the log_events.log. It keeps track of the number of batches that have been processed / to be processed.

.. code-block:: python

    dest_zarr = "tutorial_resampled_bathymetry.zarr"
    down_scale_in_batches(
        my_store=my_store,
        ds=ds,
        dest_zarr=dest_zarr,
        variables=var,
        **params)

Inspect logs
^^^^^^^^^^^^

down_scale_in_batches is a function that might take some time. In order to monitor progress, tt will log several parameters.
In case your program crashes, you can plot the logs and inspect resource consumption in order to provide better settings for a retry.

.. code-block:: python

    plot_logs.plot_logs(show=True)


Make data public accessible
^^^^^^^^^^^^^^^^^^^^^^^^^^^
Run this command in a terminal in the datalab to make your dataset public
available:

.. code-block:: console

    mc anonymous set public "s3/oidc-willemboone/tutorial_resampled_bathymetry.zarr"

This is the link of your dataset:
    https://minio.lab.dive.edito.eu/oidc-willemboone/EDITO_DUC/tutorial_resampled_bathymetry.zarr

