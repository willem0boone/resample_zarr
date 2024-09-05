import os.path
import xarray as xr
import pandas as pd

from resampling.load import write_zarr_s3
from resampling.plot_zarr import plot_dataset
from resampling.loggers import setup_logger
from resampling.plot_logs import plot_logs
from resampling.loggers import ResourceMonitor
from resampling.down_scale import down_scale_in_batches
from resampling.down_scale import down_scale_on_the_fly
from resampling.extract import extract_private_s3_zarr


if __name__ == "__main__":

    # setup logging
    resource_monitor = ResourceMonitor()
    resource_monitor.start_monitor_resources()
    event_logger = setup_logger()

    # settings
    resampler = [
        {"dimension": "latitude",
         "range": (30, 70),
         "step": 0.1,
         "invert": True
         },
        {"dimension": "longitude",
         "range": (-10, 40),
         "step": 0.1
         },
    ]

    # -------------------------------------------------------------------------
    # downscale small datastes on the fly
    datasets = pd.read_csv("datasets.csv")

    for i, item in enumerate(datasets.itertuples()):
        print(f"working on dataset {i+1} out of  {len(datasets)}: {item}")
        event_logger.info(f"Downscaling dataset: {item.dataset}")
        ds = xr.open_zarr(item.url)
        ds_downscaled = down_scale_on_the_fly(
            ds=ds,
            resampler=resampler
        )

        write_zarr_s3(dataset=ds_downscaled,
                      name=f"EDITO_DUC_{item.dataset}.zarr")

    # -------------------------------------------------------------------------
    # downscale big dataset in batches
    print("working on bathymetry")

    url = ("https://s3.waw3-1.cloudferro.com/emodnet/bathymetry/bathymetry_"
           "2022.zarr")
    var = ["elevation"]
    dest_zarr = "EDITO_DUC_bathymetry.zarr"

    event_logger.info(f"Downscaling dataset: {url}")
    ds = xr.open_zarr(url)

    params = {"resampler": resampler,
              "workers": 50,
              "batch_size": 500
              }
    down_scale_in_batches(ds=ds,
                          dest_zarr=dest_zarr,
                          variables=var,
                          logger=event_logger,
                          **params)

    # -------------------------------------------------------------------------
    print("start plotting")
    # verify datasets by plotting them
    if not os.path.isdir("plots"):
        os.makedirs("plots")

    for item in datasets.itertuples():
        print(item)
        my_s3_ds = extract_private_s3_zarr(
            name=f"EDITO_DUC_{item.dataset}.zarr")

        for time_index in range(len(my_s3_ds.time)):
            data_slice = my_s3_ds.isel(time=time_index)
            time_value = my_s3_ds.time.values[time_index]
            time_value = pd.Timestamp(time_value).year
            plot_dataset(data_slice,
                      var=item.var,
                      name=f"plots/{item.var}_{time_value}.png")
    # -------------------------------------------------------------------------
    # plot_logs logs
    plot_logs()
