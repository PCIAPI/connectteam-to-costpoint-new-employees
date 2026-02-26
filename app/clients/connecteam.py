from datetime import datetime
from typing import Any

import httpx
import stamina
import structlog

from app.models.model import ConnecteamConfig, EmployeeRecord, WorkforceRecord

logger = structlog.get_logger(__name__)

HTTP_TIMEOUT = 300.0

# Connecteam custom field IDs
CF_CP_ID = 15329039     # Costpoint EMPL_ID  (dedup key)
CF_HIRE_DATE = 4360693  # Employment Start Date (ORIG_HIRE_DT)
CF_BIRTH_DATE = 4360698 # Birth Date           (BIRTH_DT)
CF_BRANCH = 4360696     # Branch               (PROJ_ID)
CF_TEAM = 4360694       # Team
CF_TITLE = 4360692      # Title / Labor Cat    (DFLT_FL=Y BILL_LAB_CAT_CD)
CF_ORG = 4360695        # ORG Name             (PROJ_NAME)


def format_date(iso_date: str) -> str:
    """Convert '2011-07-11T00:00:00' → '07/11/2011' for Connecteam."""
    return datetime.fromisoformat(iso_date).strftime("%m/%d/%Y")


def get_team(proj_id: str) -> str:
    """Map project ID prefix to Connecteam team name."""
    return "FL Security 2025" if proj_id.startswith("2392") else "FL Baker 2025"


class ConnecteamAPIClient:
    """Handles Connecteam user read and create operations."""

    def __init__(self, config: ConnecteamConfig) -> None:
        self.config = config
        self.client = httpx.Client(
            timeout=HTTP_TIMEOUT,
            headers={
                "X-API-KEY": config.api_key,
                "accept": "application/json",
                "content-type": "application/json",
            },
        )

    def close(self) -> None:
        self.client.close()

    @stamina.retry(on=httpx.TimeoutException, attempts=3)
    def _get_users_page(self, user_status: str, offset: int, limit: int = 500) -> dict[str, Any]:
        response = self.client.get(
            self.config.users_base_url,
            params={"limit": limit, "offset": offset, "userStatus": user_status},
            timeout=30.0,
        )
        response.raise_for_status()
        return response.json()

    def _collect_cp_ids_by_status(self, user_status: str) -> set[str]:
        """Page through all users of a given status and return their CP IDs."""
        cp_ids: set[str] = set()
        offset = 0

        while True:
            logger.info("fetching_connecteam_users", status=user_status, offset=offset)
            data = self._get_users_page(user_status, offset)
            users = data.get("data", {}).get("users", [])

            if not users:
                logger.info("no_more_users", status=user_status, total=len(cp_ids))
                break

            for user in users:
                for cf in user.get("customFields", []):
                    if cf.get("customFieldId") == CF_CP_ID and cf.get("value"):
                        cp_ids.add(str(cf["value"]))

            paging = data.get("paging", {})
            next_offset = paging.get("offset")
            if next_offset is None or next_offset <= offset:
                break
            offset = next_offset

        return cp_ids

    def get_existing_cp_ids(self) -> set[str]:
        """Return CP IDs for all Connecteam users (active + archived)."""
        active_ids = self._collect_cp_ids_by_status("active")
        archived_ids = self._collect_cp_ids_by_status("archived")
        all_ids = active_ids | archived_ids
        logger.info(
            "existing_cp_ids_total",
            active=len(active_ids),
            archived=len(archived_ids),
            total=len(all_ids),
        )
        return all_ids

    @staticmethod
    def build_payload(workforce: WorkforceRecord, employee: EmployeeRecord) -> dict[str, Any]:
        """Build the Connecteam user creation payload without sending it."""
        custom_fields: list[dict[str, Any]] = [
            {"customFieldId": CF_CP_ID, "value": employee.empl_id},
            {"customFieldId": CF_TITLE, "value": workforce.bill_lab_cat_cd},
            {"customFieldId": CF_BRANCH, "value": workforce.proj_id},
            {"customFieldId": CF_TEAM, "value": get_team(workforce.proj_id)},
            {"customFieldId": CF_ORG, "value": workforce.proj_name},
        ]

        if employee.orig_hire_dt:
            custom_fields.append({
                "customFieldId": CF_HIRE_DATE,
                "value": format_date(employee.orig_hire_dt),
            })
        if employee.birth_dt:
            custom_fields.append({
                "customFieldId": CF_BIRTH_DATE,
                "value": format_date(employee.birth_dt),
            })

        payload: dict[str, Any] = {
            "userType": "user",
            "isArchived": False,
            "firstName": employee.first_name,
            "lastName": employee.last_name,
            "customFields": custom_fields,
        }
        if employee.home_email_id:
            payload["email"] = employee.home_email_id
        else:
            logger.warning("no_home_email", empl_id=employee.empl_id)

        return payload

    @stamina.retry(on=httpx.TimeoutException, attempts=3)
    def post_user(self, workforce: WorkforceRecord, employee: EmployeeRecord) -> dict[str, Any]:
        """Create a single user in Connecteam. Returns a result dict."""
        payload = self.build_payload(workforce, employee)
        logger.info(
            "posting_user",
            empl_id=employee.empl_id,
            name=f"{employee.first_name} {employee.last_name}",
            proj_id=workforce.proj_id,
        )

        try:
            response = self.client.post(
                self.config.users_base_url,
                params={"sendActivation": "false"},
                json=[payload],
                timeout=30.0,
            )
            response.raise_for_status()
            logger.info("user_created", empl_id=employee.empl_id, status_code=response.status_code)
            return {
                "success": True,
                "empl_id": employee.empl_id,
                "name": f"{employee.first_name} {employee.last_name}",
                "response": response.json(),
            }

        except httpx.HTTPStatusError as exc:
            error_detail: Any = None
            try:
                error_detail = exc.response.json()
            except Exception:
                error_detail = exc.response.text
            logger.error(
                "user_create_failed",
                empl_id=employee.empl_id,
                status_code=exc.response.status_code,
                detail=error_detail,
            )
            return {
                "success": False,
                "empl_id": employee.empl_id,
                "name": f"{employee.first_name} {employee.last_name}",
                "error": str(exc),
                "detail": error_detail,
            }
