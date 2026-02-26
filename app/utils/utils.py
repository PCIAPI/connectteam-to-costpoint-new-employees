import json
from typing import Any

import structlog

from app.services.aws import s3_client

logger = structlog.get_logger()

S3_BUCKET = "gardaworld"
S3_KEY_PREFIX = "new-employees"


def save_json(data: Any, filename: str) -> None:
    """Serialize data to JSON and upload to S3."""
    body = json.dumps(data, indent=2, default=str)
    s3_key = f"{S3_KEY_PREFIX}/{filename}"
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("file_saved_to_s3", bucket=S3_BUCKET, key=s3_key)


def save_html(html: str, filename: str) -> None:
    """Upload an HTML string to S3."""
    s3_key = f"{S3_KEY_PREFIX}/{filename}"
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=html.encode("utf-8"),
        ContentType="text/html",
    )
    logger.info("file_saved_to_s3", bucket=S3_BUCKET, key=s3_key)
