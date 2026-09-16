"""Shared test fixtures."""

import os
from collections.abc import Iterator

import boto3
import pytest
from moto import mock_aws

BUCKET = 'test-bucket'
REGION = 'ap-southeast-2'


@pytest.fixture(scope='module')
def _credentials() -> Iterator[None]:
    """Set dummy AWS credentials so moto never reaches real AWS."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = REGION
    yield


@pytest.fixture
def _bucket(_credentials: None) -> Iterator[str]:
    """Create an empty S3 bucket for the duration of one test."""
    with mock_aws():
        client = boto3.client('s3', region_name=REGION)
        client.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={'LocationConstraint': REGION},
        )
        yield BUCKET
