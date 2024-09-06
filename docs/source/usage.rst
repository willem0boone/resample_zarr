Usage
=====

.. _installation:

Installation
------------

To be developed.

Resampling datasets
-------------------

# Resample ZARR datasets
This notebook gives a demonstration of how to resample zarr datasets.
## TOC:
* [1. Import packages](#1)
* [2. Settings](#2)
* [3. Resample on the fly](#3)
* [4. Resample using batches](#4)

## 1. Import packages <a class="anchor" id="1"></a>
#### Import standard packages

.. code-block:: python
    import sys
    import os.path
    import xarray as xr
    import pandas as pd
    from pathlib import Path

#### Import Resampling modules
Before running this code, make sure your S3 credentials are stored in the config.toml. 

.. code-block:: python
    parent_dir = Path().resolve().parent
    sys.path.append(str(parent_dir))
    from resampling import load
    from resampling import extract
    from resampling import down_scale
    from resampling import loggers
    from resampling import plot_logs

## 2. Settings <a class="anchor" id="2"></a>
#### Target resolution <a class="anchor" id="2.0"></a>
Define the dimesion, ranges and resolution (step) to which the dataset should be resampled. <br>
Note that dimensions that are in the dataset but that are not mentioned in resampler will be maintained to the original resolution.

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

#### Initiate loggers for monitoring
resource_logger: logs every 60 seconds the resources
* number of threads
* RAM
* CPU

.. code-block:: python
    resource_monitor = loggers.ResourceMonitor()
    resource_monitor.start_monitor_resources()

event_logger will return an initiation of logging.Logger to be used for logging events later on.

.. code-block:: python
    event_logger = loggers.setup_logger()

## 3. Resample on the fly <a class="anchor" id="3"></a>
If a dataset is manageable to be read in momory without consuming to much RAM, it can be resampled in on the fly.
#### Define target dataset

.. code-block:: python
    url = "https://s3.waw3-1.cloudferro.com/emodnet/emodnet_arco/bio_oracle/sea_water_temperature/sea_water_temperature_bio_oracle_baseline_2000_2019/climatologydecadedepthsurf.zarr"
    var = "average_sea_water_temperature_biooracle_baseline"

Open the dataset with xarray

.. code-block:: python
    ds = xr.open_zarr(url)
    print(ds)

#### Rescale dataset
Rescale the xarray dataset according the information from the resampler.

.. code-block:: python
    ds_downscaled = down_scale.down_scale_on_the_fly(
        ds=ds,
        resampler=resampler
    )

.. code-block:: python
    print(ds_downscaled)

.. code-block:: python
    ds_name = "tutorial_resampled_SST_baseline"
    load.write_zarr_s3(dataset=ds_downscaled,
                       name=f"{ds_name}.zarr")

## 4. Resample using batches <a class="anchor" id="4"></a>
When reading datasets that consume more RAM then you have available, it is not feasible to process them in memory. Instead, the dataset will be devided in batches (each containing #x windows) and each window will be processed seperate. 
### Define target dataset 

.. code-block:: python
    url = ("https://s3.waw3-1.cloudferro.com/emodnet/bathymetry/bathymetry_"
               "2022.zarr")
    var = ["elevation"]

Open the dataset with xarray, notice that this ds consumes 66GB.

.. code-block:: python
    ds = xr.open_zarr(url)
    print(ds)

Additional settings to regulate the window processing:
- workers: number of windows that will be processed in parallel. Each window will start a thread. Notice that the total number of thread will be larger than number of workers due to a number of background threads (especially since zarr is using Dask which will also open several threads).
- batch_size: number of windows to be processed before writing the results to the zarr. Small batch size will slow down the process because of the increasing amount of write operations. Large batch sizes could cause RAM errors.
#### Batch processing settings

.. code-block:: python
    params = {"resampler": resampler,
              "workers": 50,
              "batch_size": 500
             }

#### Process dataset
Attention! This step will take some time.<br>
You can track progress in the log_events.log. It keeps track of the number of batches that have been processed / to be processed.

.. code-block:: python
    dest_zarr = "tutorial_resampled_bathymetry.zarr"
    down_scale.down_scale_in_batches(ds=ds,
                                     dest_zarr=dest_zarr,
                                     variables=var,
                                     logger=event_logger,
                                     **params)

#### Inspect logs
In case your program crashes, you can plot the logs and inspect resource consumption in order to provide better settings for a retry.

.. code-block:: python
    plot_logs.plot_logs(show=True)



