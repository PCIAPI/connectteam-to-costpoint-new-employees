from datetime import datetime

from pci_telemetry.events.sqs import SQSEventSender

from app.services.aws import lambda_client


class EventService:

    def __init__(self, client_name: str, function_name: str, app_config_dict: dict | None = None):
        self.sender = SQSEventSender(organization_id=client_name, source_id=function_name)
        self.function_name = function_name
        self.client_name = client_name
        self.recipients: list[str] = []
        if app_config_dict is not None:
            self.recipients = app_config_dict.get("email_list", [])

    def _details(self, message: str) -> dict:
        return {"message": message}

    def log_info(self, log_type: str, message: str) -> None:
        print(message)
        self.sender.info(f"new_employees_sync.{log_type}", details=self._details(message))

    def log_error(self, log_type: str, message: str) -> None:
        print(message)
        self.sender.error(f"new_employees_sync.{log_type}", details=self._details(message))

    def log_success(self, log_type: str, message: str) -> None:
        print(message)
        self.sender.success(f"new_employees_sync.{log_type}", details=self._details(message))


# ---------------------------------------------------------------------------
# Global singleton
# ---------------------------------------------------------------------------

_instance: "EventService | None" = None


def init_event_service(
    client_name: str,
    function_name: str,
    app_config_dict: dict | None = None,
) -> EventService:
    global _instance
    _instance = EventService(client_name, function_name, app_config_dict)
    return _instance


def get_event_service() -> EventService:
    if _instance is None:
        raise RuntimeError(
            "EventService has not been initialized. "
            "Call init_event_service() before logging."
        )
    return _instance
