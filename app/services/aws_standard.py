import os

import boto3
from botocore.config import Config

aws_session_params = {
    "aws_access_key_id": os.environ.get("AWS_STANDARD_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.environ.get("AWS_STANDARD_ACCESS_KEY_SECRET"),
    "region_name": "us-east-1",
}
aws_session = boto3.Session(**aws_session_params)

ses_client = aws_session.client("ses", region_name="us-east-1")
