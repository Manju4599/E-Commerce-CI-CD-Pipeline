
import json
from botocore.exceptions import ClientError

from src.s3_ingestion import s3, BUCKET_NAME


MANIFEST_KEY = "silver/orders/manifest.json"


def read_silver_manifest(
    manifest_key: str = MANIFEST_KEY,
):
    try:
        response = s3.get_object(
            Bucket=BUCKET_NAME,
            Key=manifest_key,
        )

    except ClientError as error:
        error_code = error.response.get(
            "Error", {}
        ).get("Code")

        if error_code in ("NoSuchKey", "404", "NotFound"):
            return None

        raise

    manifest_content = response["Body"].read()

    return json.loads(
        manifest_content.decode("utf-8")
    )


def write_silver_manifest(
    manifest,
    manifest_key: str = MANIFEST_KEY,
):
    required_fields = {
        "silver_version",
        "file",
        "sha256",
        "last_processed_order_id",
        "committed_batches",
    }

    missing_fields = required_fields - manifest.keys()

    if missing_fields:
        raise ValueError(
            f"Missing manifest fields: {missing_fields}"
        )

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=manifest_key,
        Body=json.dumps(
            manifest,
            indent=4,
        ).encode("utf-8"),
        ContentType="application/json",
    )
