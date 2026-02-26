import os

import boto3
from aws_lambda_powertools.utilities.parameters import SecretsProvider
from botocore.config import Config

aws_session_params = {
    "aws_access_key_id": os.environ.get("AWS_GOV_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.environ.get("AWS_GOV_ACCESS_KEY_SECRET"),
    "region_name": "us-gov-east-1",
}
aws_session = boto3.Session(**aws_session_params)

s3_session_params = {**aws_session_params, "config": boto3.session.Config(signature_version="s3v4")}
s3_client = boto3.client("s3", **s3_session_params)

secrets_provider = SecretsProvider(
    Config(connect_timeout=1, retries={"total_max_attempts": 2, "max_attempts": 5}),
    boto3_session=aws_session,
)
secrets_client = aws_session.client(
    service_name="secretsmanager",
    region_name="us-gov-east-1",
)
lambda_client = aws_session.client("lambda", region_name="us-gov-east-1")
