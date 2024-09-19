"""old code"""
# import unittest
# import os
# from typing import Optional
# from typing import Any
# from typing import Dict
# import shutil
# from down_scale import define_windows
# from down_scale import down_scale_local
# import xarray as xr
#
#
# PUBLIC_ZARR = ("https://s3.waw3-1.cloudferro.com/emodnet/emodnet_arco/"
#                "bio_oracle/sea_water_temperature/sea_water_temperature_bio_"
#                "oracle_scenario_ssp245_2020_2100/climatologyDecadeDepthSurf"
#                ".zarr")
# PUBLIC_ZARR_VARS = ["average_sea_water_temperature_biooracle_ssp245"]
#
# BATCH_SIZE = 100
# WORKERS = 20
#
# RESAMPLER = (
#         {"dimension": "latitude", "range": (50, 60), "step": 0.5,
#          "invert": True},
#         {"dimension": "longitude", "range": (-10, 10), "step": 0.5},
#         {"dimension": "time", "range": (2030)},
# )
#
#
# class TestDefineWindows(unittest.TestCase):
#     def test_basic_intervals(self):
#         resampler = [
#             {"dimension": "latitude", "range": (50, 60), "step": 5},
#             {"dimension": "longitude", "range": (-10, 10), "step": 10}
#         ]
#
#         expected_dims_with_coords = [
#             {"latitude": [50, 55], "longitude": [-10, 0]},
#             {"latitude": [50, 55], "longitude": [0, 10]},
#             {"latitude": [55, 60], "longitude": [-10, 0]},
#             {"latitude": [55, 60], "longitude": [0, 10]}
#         ]
#
#         expected_dims_with_indices = [
#             {"latitude": 0, "longitude": 0},
#             {"latitude": 0, "longitude": 1},
#             {"latitude": 1, "longitude": 0},
#             {"latitude": 1, "longitude": 1}
#         ]
#
#         expected_dimensions = {
#             "latitude": [[50, 55], [55, 60]],
#             "longitude": [[-10, 0], [0, 10]]
#         }
#
#         dims_with_coords, dims_with_indices, dimensions = define_windows(
#             resampler)
#
#         self.assertListEqual(dims_with_coords, expected_dims_with_coords)
#         self.assertListEqual(dims_with_indices, expected_dims_with_indices)
#         self.assertDictEqual(dimensions, expected_dimensions)
#
#     def test_single_value_range(self):
#         resampler = [
#             {"dimension": "time", "range": 2030}
#         ]
#
#         expected_dims_with_coords = [{"time": 2030}]
#         expected_dims_with_indices = [{"time": 0}]
#         expected_dimensions = {
#             "time": [2030]
#         }
#
#         dims_with_coords, dims_with_indices, dimensions = define_windows(
#             resampler)
#
#         self.assertListEqual(dims_with_coords, expected_dims_with_coords)
#         self.assertListEqual(dims_with_indices, expected_dims_with_indices)
#         self.assertDictEqual(dimensions, expected_dimensions)
#
#     def test_inverted_intervals(self):
#         def test_inverted_intervals(self):
#             resampler = [
#                 {"dimension": "latitude", "range": (50, 60), "step": 5,
#                  "invert": True}
#             ]
#
#             expected_dims_with_coords = [
#                 {"latitude": [55, 60]},
#                 {"latitude": [50, 55]}
#             ]
#             expected_dims_with_indices = [
#                 {"latitude": 0},
#                 {"latitude": 1}
#             ]
#             expected_dimensions = {
#                 "latitude": [[55, 60], [50, 55]]
#             }
#
#             dims_with_coords, dims_with_indices, dimensions = define_windows(
#                 resampler)
#
#             self.assertListEqual(dims_with_coords, expected_dims_with_coords)
#             self.assertListEqual(dims_with_indices, expected_dims_with_indices)
#             self.assertDictEqual(dimensions, expected_dimensions)
#
#     def test_step_larger_than_range(self):
#         resampler = [
#             {"dimension": "longitude", "range": (0, 5), "step": 10}
#         ]
#
#         expected_dims_with_coords = [{"longitude": [0, 5]}]
#         expected_dims_with_indices = [{"longitude": 0}]
#         expected_dimensions = {
#             "longitude": [[0, 5]]
#         }
#
#         dims_with_coords, dims_with_indices, dimensions = define_windows(
#             resampler)
#
#         self.assertListEqual(dims_with_coords, expected_dims_with_coords)
#         self.assertListEqual(dims_with_indices, expected_dims_with_indices)
#         self.assertDictEqual(dimensions, expected_dimensions)
#
#
# class TestDownscale(unittest.TestCase):
#     def setUp(self,
#               url: Optional[str] = PUBLIC_ZARR,
#               batch_size: Optional[int] = BATCH_SIZE,
#               workers: Optional[int] = WORKERS,
#               resampler: Optional[tuple[Dict[str, Any]]] = RESAMPLER,
#               variables: Optional[list[str, ...]] = PUBLIC_ZARR_VARS):
#         """
#         Set up the test environment by initializing test parameters.
#
#         :param url: The URL of the source Zarr dataset.
#         :param batch_size: The size of the batches to process.
#         :param workers: The number of workers to use.
#         :param resampler: The configuration for resampling.
#         :param variables: The variables to be used in the downscale function.
#         """
#         self.url = url
#         self.batch_size = batch_size
#         self.workers = workers
#         self.resampler = resampler
#         self.variables = variables
#
#     def test_downscale(self):
#         """
#         Test the downscale function with the parameters initialized in setUp.
#         """
#         zarr = "test.zarr"
#         if os.path.exists(zarr):
#             shutil.rmtree(zarr)
#
#         ds = xr.open_zarr(self.url)
#
#         down_scale_local(
#             ds=ds,
#             target_zarr=zarr,
#             resampler=self.resampler,
#             variables=self.variables,
#             batch_size=self.batch_size,
#             workers=self.workers
#         )
#
#
# if __name__ == "__main__":
#     unittest.main()
#
