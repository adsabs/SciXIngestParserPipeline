import os
from unittest.mock import patch

import boto3
import moto
import pytest
from botocore.exceptions import ParamValidationError
from SciXPipelineUtils.s3_methods import load_s3_providers


@pytest.fixture
def empty_bucket():
    """
    Generates an empty S3 bucket using the moto mocking framework.
    """
    moto_fake = moto.mock_s3()
    try:
        moto_fake.start()
        conn = boto3.resource("s3")
        conn.create_bucket(Bucket="test-bucket-name")  # or the name of the bucket you use
        yield conn
    finally:
        moto_fake.stop()


def test_upload_object(empty_bucket):
    """
    Tests S3 methods for AWS host
    """
    mock_config = {"S3_PROVIDERS": ["AWS"], "AWS_BUCKET_NAME": "test-bucket-name"}
    buckets = load_s3_providers(mock_config)
    file_bytes = b"Test_text"
    object_name = "/test_object/name"
    for producer in buckets:
        buckets[producer].write_object_s3(file_bytes=file_bytes, object_name=object_name)


def test_upload_nonbytes_object(empty_bucket):
    """
    Tests error handling for file type
    """
    mock_config = {"S3_PROVIDERS": ["AWS"], "AWS_BUCKET_NAME": "test-bucket-name"}
    buckets = load_s3_providers(mock_config)
    file_bytes = 11
    object_name = "/test_object/name"
    with pytest.raises(ParamValidationError):
        for producer in buckets:
            buckets[producer].write_object_s3(file_bytes=file_bytes, object_name=object_name)


@moto.mock_s3
def test_alternate_s3_endpoint_put_object():
    """
    Test for checking that alternate providers work with the S3 implementation.
    """
    provider = "ALT_S3_PROVIDER_1"
    url = "https://test.bucket.domain"
    mock_config = {
        "S3_PROVIDERS": [provider],
        str(provider) + "_BUCKET_NAME": "test-bucket-name",
        str(provider) + "_S3_URL": url,
    }
    with patch.dict(os.environ, {"MOTO_S3_CUSTOM_ENDPOINTS": url}):
        with moto.mock_s3():
            bucket = mock_config.get(str(provider) + "_BUCKET_NAME")
            conn = boto3.resource("s3", endpoint_url=url)
            conn.create_bucket(Bucket=bucket)

            buckets = load_s3_providers(mock_config)
            file_bytes = b"Test_text"
            object_name = "/test_object/name"
            for producer in buckets:
                buckets[producer].write_object_s3(file_bytes=file_bytes, object_name=object_name)
