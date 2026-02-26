import base64
import json

import structlog
from botocore.exceptions import ClientError

from app.services.aws import secrets_client

logger = structlog.get_logger()


class SecretsService:
    def __init__(self, client_name: str):
        self.client_name = client_name

    def get_secret(self, secret_name: str) -> dict:
        try:
            response = secrets_client.get_secret_value(SecretId=secret_name)
        except ClientError as e:
            logger.error("secrets_client_error", secret_name=secret_name, error=str(e))
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                raise ValueError(f"Secret '{secret_name}' not found.") from e
            raise

        if "SecretString" in response:
            secret = response["SecretString"]
        elif "SecretBinary" in response:
            secret = base64.b64decode(response["SecretBinary"]).decode("utf-8")
        else:
            raise ValueError("SecretString and SecretBinary are both missing from the response.")

        try:
            return json.loads(secret)
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to decode secret as JSON: {secret}") from e
