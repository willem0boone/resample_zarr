import unittest
import boto3
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import PartialCredentialsError
from botocore.exceptions import ClientError
from config import Config


class TestConfig(unittest.TestCase):
    def test_object(self):
        try:
            config = Config()
        except Exception as e:
            self.fail(f"Creating Config instance failed with exception: {e}")

    def test_s3_connection(self):
        try:
            settings = Config().settings
            # Create an S3 client
            s3_client = boto3.client(
                's3',
                endpoint_url=settings.endpoint_url,
                aws_access_key_id=settings.aws_access_key_id,
                aws_secret_access_key=settings.aws_secret_access_key,
                aws_session_token=settings.aws_session_token
            )

            # Test listing buckets
            response = s3_client.list_buckets()
            self.assertIn('Buckets', response)

            # Optionally check if a specific bucket exists
            bucket_exists = any(bucket['Name'] == settings.bucket for bucket in
                                response.get('Buckets', []))
            self.assertTrue(bucket_exists,
                            f"Bucket {settings.bucket} does not exist.")

        except (NoCredentialsError, PartialCredentialsError) as e:
            self.fail(f"Credentials error: {e}")
        except ClientError as e:
            self.fail(f"Client error: {e}")
        except Exception as e:
            self.fail(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    unittest.main()

