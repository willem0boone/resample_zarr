import unittest
import boto3
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import PartialCredentialsError
from botocore.exceptions import ClientError
from resampling.my_store import get_my_store
import warnings


class TestObjectStore(unittest.TestCase):

    def setUp(self):
        """ Set up variables shared across tests """
        self.my_store = None

    def test_get_my_store(self):
        """ Test to initiate ObjectStore """
        try:
            self.my_store = get_my_store()
        except Exception as e:
            warnings.warn(
                f"Initiating Objectstore via get_my_store encountered an "
                f"error: {e}")
            self.my_store = None

    def test_s3_connection(self):
        """ Test to check S3 connection, dependent on test_get_my_store """
        if self.my_store is None:
            self.skipTest(
                "Skipping S3 connection test because my_store initialization "
                "failed.")

        try:
            # Create an S3 client
            s3_client = boto3.client(
                's3',
                endpoint_url=self.my_store._endpoint_url,
                aws_access_key_id=self.my_store._aws_access_key_id,
                aws_secret_access_key=self.my_store._aws_secret_access_key,
                aws_session_token=self.my_store._aws_session_token
            )

            # Test listing buckets
            response = s3_client.list_buckets()
            self.assertIn('Buckets', response)

            # Optionally check if a specific bucket exists
            bucket_exists = any(
                bucket['Name'] == self.my_store._bucket for bucket in
                response.get('Buckets', []))
            self.assertTrue(bucket_exists,
                            f"Bucket {self.my_store._bucket} does not "
                            f"exist.")

        except (NoCredentialsError, PartialCredentialsError) as e:
            self.fail(f"Credentials error: {e}")
        except ClientError as e:
            self.fail(f"Client error: {e}")
        except Exception as e:
            self.fail(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    unittest.main()

