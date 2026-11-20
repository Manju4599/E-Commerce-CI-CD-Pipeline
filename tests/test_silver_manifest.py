
import json
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError

from src.silver_manifest import (
    read_silver_manifest,
    write_silver_manifest,
    MANIFEST_KEY,
    BUCKET_NAME,
)


@pytest.fixture
def sample_manifest():
    return {
        "silver_version": 1,
        "file": "silver/orders/orders_v001.csv",
        "sha256": "a" * 64,
        "last_processed_order_id": 112,
        "committed_batches": [
            "orders_109_109",
            "orders_110_112",
        ],
    }


def test_read_existing_manifest(sample_manifest):

    response = {
        "Body": type(
            "MockBody",
            (),
            {
                "read": lambda self: json.dumps(
                    sample_manifest
                ).encode("utf-8")
            },
        )()
    }

    with patch(
        "src.silver_manifest.s3.get_object",
        return_value=response,
    ) as mock_get:

        result = read_silver_manifest()

    assert result == sample_manifest

    mock_get.assert_called_once_with(
        Bucket=BUCKET_NAME,
        Key=MANIFEST_KEY,
    )


def test_read_missing_manifest():

    error = ClientError(
        {
            "Error": {
                "Code": "NoSuchKey",
                "Message": "Not found",
            }
        },
        "GetObject",
    )

    with patch(
        "src.silver_manifest.s3.get_object",
        side_effect=error,
    ):

        result = read_silver_manifest()

    assert result is None


def test_write_manifest(sample_manifest):

    with patch(
        "src.silver_manifest.s3.put_object"
    ) as mock_put:

        write_silver_manifest(sample_manifest)

    mock_put.assert_called_once()

    call_args = mock_put.call_args.kwargs

    assert call_args["Bucket"] == BUCKET_NAME
    assert call_args["Key"] == MANIFEST_KEY
    assert call_args["ContentType"] == "application/json"

    uploaded_manifest = json.loads(
        call_args["Body"].decode("utf-8")
    )

    assert uploaded_manifest == sample_manifest


def test_write_manifest_rejects_missing_fields():

    invalid_manifest = {
        "silver_version": 1,
    }

    with pytest.raises(
        ValueError,
        match="Missing manifest fields",
    ):
        write_silver_manifest(invalid_manifest)


def test_read_manifest_propagates_unexpected_s3_error():

    error = ClientError(
        {
            "Error": {
                "Code": "AccessDenied",
                "Message": "Access denied",
            }
        },
        "GetObject",
    )

    with patch(
        "src.silver_manifest.s3.get_object",
        side_effect=error,
    ):

        with pytest.raises(ClientError):
            read_silver_manifest()

