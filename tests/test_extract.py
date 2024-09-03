import unittest
from typing import Optional
from extract import extract_public_s3_zarr
from extract import extract_private_s3_zarr
from extract import extract_web_nc
from extract import  check_s3_zarr_exists

PUBLIC_ZARR = ("https://s3.waw3-1.cloudferro.com/emodnet/bathymetry/"
               "bathymetry_2022.zarr/")
PUBLIC_ZARR_VAR = "elevation"

PRIVATE_ZARR = "hidden_zarr.zarr"
PRIVATE_ZARR_VAR = "elevation"

PUBLIC_NC = ("https://erddap.bio-oracle.org/erddap/files/"
             "thetao_baseline_2000_2019_depthmax/climatologydecadedepthmax.nc")

PUBLIC_NC_VAR = "thetao_mean"

EXISTING_ZARR = "hidden_zarr.zarr"


class TestExtractPublicZarr(unittest.TestCase):
    def setUp(self,
              public_zarr: Optional[str] = PUBLIC_ZARR,
              public_zarr_var: Optional[str] = PUBLIC_ZARR_VAR,
              ):
        self.public_zarr = public_zarr
        self.public_zarr_var = public_zarr_var

    def test_variable_extraction(self):
        result_ds = extract_public_s3_zarr(
            url=self.public_zarr,
            var=self.public_zarr_var
        )

    def test_lat_lon_selection(self):
        test_lat_range = (50, 55)
        test_lon_range = (0, 5)

        result_ds = extract_public_s3_zarr(
            url=self.public_zarr,
            lon_range=test_lon_range,
            lat_range=test_lat_range
        )

    def test_no_variable(self):
        result_ds = extract_public_s3_zarr(
            url=self.public_zarr,
            var=None
        )

    def test_variable_does_not_exist(self):
        with self.assertRaises(ValueError) as context:
            extract_public_s3_zarr(
                url=self.public_zarr,
                var='non_existent_var'
            )

        self.assertEqual(
            str(context.exception),
            "Variable 'non_existent_var' not found in the dataset.")


class TestExtractPrivateZarr(unittest.TestCase):
    def setUp(self,
                 private_zarr: Optional[str] = PRIVATE_ZARR,
                 private_zarr_var: Optional[str] = PRIVATE_ZARR_VAR,
                 ):
        self.private_zarr = private_zarr
        self.private_zarr_var = private_zarr_var

    def test_variable_extraction(self):
        test_name = "hidden_zarr.zarr"
        test_var = "elevation"

        ds = extract_private_s3_zarr(name=test_name,
                                     var=test_var,
                                     )

    def test_lat_lon_selection(self):
        test_lon_range = [0, 5]
        test_lat_range = [50, 55]

        ds = extract_private_s3_zarr(name=self.private_zarr,
                                     lon_range=test_lon_range,
                                     lat_range=test_lat_range)

    def test_no_variable(self):
        ds = extract_private_s3_zarr(name=self.private_zarr)

    def test_variable_does_not_exist(self):
        with self.assertRaises(ValueError) as context:
            extract_private_s3_zarr(
                name=self.private_zarr,
                var='non_existent_var'
            )
        self.assertEqual(
            str(context.exception),
            "Variable 'non_existent_var' not found in the dataset.")


class TestExtractNc(unittest.TestCase):
    def setUp(self,
              public_nc: Optional[str] = PUBLIC_NC,
              public_nc_var: Optional[str] = PUBLIC_NC_VAR,
              ):
        self.public_nc = public_nc
        self.public_nc_var = public_nc_var

    def test_variable_extraction(self):
        test_lon_range = [0, 5]
        test_lat_range = [50, 55]

        ds = extract_web_nc(url=self.public_nc,
                            var=self.public_nc_var,
                            lon_range=test_lon_range,
                            lat_range=test_lat_range,
        )

    def test_lat_lon_selection(self):
        pass

    def test_no_variable(self):
        pass

    def test_variable_does_not_exist(self):
        pass


class TestZarrExist(unittest.TestCase):
    def setUp(self,
              existing_zarr: Optional[str] = EXISTING_ZARR,
              ):
        self.zarr = existing_zarr

    def test_exist(self):
        exists = check_s3_zarr_exists(self.zarr)
        print(f"the requested zarr {self.zarr} exists: {exists}")
        assert exists is True


if __name__ == "__main__":
    unittest.main()

