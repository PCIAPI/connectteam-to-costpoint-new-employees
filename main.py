from aws_lambda_powertools.utilities.typing import LambdaContext

from app.services.sync import sync
from app.utils.observability import aws_xray_tracer


@aws_xray_tracer.capture_lambda_handler
def lambda_handler(event: dict, context: LambdaContext):
    """
    Lambda entry point.

    Event payload:
        {
            "client_name": "gardaworld",   # required
            "dry_run": false               # optional, default false
        }
    """
    sync(
        client_name=event.get("client_name", event.get("org_reference_id", "")),
        dry_run=event.get("dry_run", False),
    )
