from base64 import b64encode
from typing import Any

import httpx
import stamina
import structlog

from app.models.model import DeltekConfig, EmployeeRecord, WorkforceRecord

logger = structlog.get_logger(__name__)

HTTP_TIMEOUT = 300.0


class DeltekAPIClient:
    """Handles all Costpoint API calls."""

    def __init__(self, config: DeltekConfig) -> None:
        credentials = f"{config.username}:{config.password}"
        encoded = b64encode(credentials.encode()).decode()
        self.client = httpx.Client(
            timeout=HTTP_TIMEOUT,
            headers={
                "Authorization": f"Basic {encoded}",
                "Content-Type": "application/json",
            },
        )
        self.config = config

    def close(self) -> None:
        self.client.close()

    @stamina.retry(on=httpx.TimeoutException, attempts=3)
    def _post(self, payload: dict[str, Any], timeout: float | None = None) -> dict[str, Any]:
        response = self.client.post(
            self.config.full_url,
            json=payload,
            timeout=timeout or HTTP_TIMEOUT,
        )
        response.raise_for_status()
        return response.json()

    def get_ct_projects(self) -> dict[str, str]:
        """Return {proj_id: proj_name} for all active projects with NOTES=CT."""
        payload = {
            "filter": {
                "id": "pjmbasicrrexpt",
                "where": [{
                    "rsWhere": {
                        "rsId": "PJMBASIC_PROJ",
                        "conditions": [{
                            "joinWithParent": "N",
                            "relations": [
                                {"name": "ACTIVE_FL", "relation": "=", "value": "Y"},
                                {"name": "ALLOW_CHARGES_FL", "relation": "=", "value": "Y"},
                            ],
                        }],
                        "children": [],
                    },
                }],
            },
        }

        logger.info("fetching_ct_projects")
        data = self._post(payload)

        ct_projects: dict[str, str] = {}
        for row_obj in data.get("document", {}).get("rows", []):
            row = row_obj.get("row", {})
            if row.get("rsId") != "PJMBASIC_PROJ":
                continue
            row_data = row.get("data", {})
            proj_id = row_data.get("PROJ_ID", "")
            proj_name = row_data.get("PROJ_NAME", "")
            for child in row.get("children", []):
                child_row = child.get("row", {})
                if child_row.get("rsId") == "PJMBASIC_PROJ_NOTES":
                    if child_row.get("data", {}).get("NOTES") == self.config.filter_notes_value:
                        ct_projects[proj_id] = proj_name
                        logger.info("ct_project_found", proj_id=proj_id, proj_name=proj_name)

        logger.info("ct_projects_fetched", count=len(ct_projects))
        return ct_projects

    def get_workforce(self, proj_id: str, proj_name: str) -> list[WorkforceRecord]:
        """Return one WorkforceRecord per unique employee on this project (DFLT_FL=Y)."""
        payload = {
            "filter": {
                "id": "pjmworkrrexp",
                "where": [{
                    "rsWhere": {
                        "rsId": "PJM_PROJEMPL_HDR",
                        "conditions": [{
                            "joinWithParent": "N",
                            "relations": [
                                {"name": "PROJ_ID", "relation": "=", "value": proj_id},
                            ],
                        }],
                        "children": [
                            {"rsWhere": {"rsId": "PJM_PROJEMPL_CHILDTO", "conditions": [], "children": []}},
                            {"rsWhere": {"rsId": "PJM_PROJEMPL_LABCAT_PLCWKFRCE", "conditions": [], "children": []}},
                        ],
                    },
                }],
            },
        }

        logger.info("fetching_workforce", proj_id=proj_id)
        data = self._post(payload, timeout=30.0)

        empl_rows: dict[str, list[dict[str, Any]]] = {}
        for row_obj in data.get("document", {}).get("rows", []):
            row = row_obj.get("row", {})
            if row.get("rsId") != "PJM_PROJEMPL_HDR":
                continue
            for child in row.get("children", []):
                child_row = child.get("row", {})
                if child_row.get("rsId") != "PJM_PROJEMPL_LABCAT_PLCWKFRCE":
                    continue
                for grandchild in child_row.get("children", []):
                    gc_row = grandchild.get("row", {})
                    if gc_row.get("rsId") != "PJM_PROJEMPLLABCAT_PLCWK":
                        continue
                    d = gc_row.get("data", {})
                    empl_id = d.get("PJM_PROJEMPLLABCAT_PLCWK_EMPL_ID", "")
                    if not empl_id:
                        continue
                    if empl_id not in empl_rows:
                        empl_rows[empl_id] = []
                    empl_rows[empl_id].append(d)

        records: list[WorkforceRecord] = []
        for empl_id, rows in empl_rows.items():
            dflt = next((r for r in rows if r.get("DFLT_FL") == "Y"), None)
            if dflt:
                records.append(WorkforceRecord(
                    empl_id=empl_id,
                    proj_id=proj_id,
                    proj_name=proj_name,
                    bill_lab_cat_cd=dflt["PJM_PROJEMPLLABCAT_PLCWK_BILL_LAB_CAT_CD"],
                ))

        logger.info("workforce_fetched", proj_id=proj_id, employee_count=len(records))
        return records

    def get_employee(self, empl_id: str) -> EmployeeRecord | None:
        """Fetch a single employee's details from Costpoint by EMPL_ID."""
        payload = {
            "filter": {
                "id": "ldmeinforrexpt",
                "where": [{
                    "rsWhere": {
                        "rsId": "LDMEINFO_EMPL",
                        "conditions": [{
                            "joinWithParent": "N",
                            "relations": [
                                {"name": "EMPL_ID", "relation": "=", "value": empl_id},
                            ],
                        }],
                        "children": [],
                    },
                }],
            },
        }

        try:
            data = self._post(payload, timeout=30.0)
        except httpx.HTTPStatusError as exc:
            logger.error("employee_fetch_failed", empl_id=empl_id, status=exc.response.status_code)
            return None

        rows = data.get("document", {}).get("rows", [])
        if not rows:
            logger.warning("employee_not_found_in_costpoint", empl_id=empl_id)
            return None

        row_data = rows[0].get("row", {}).get("data", {})
        return EmployeeRecord(
            empl_id=empl_id,
            first_name=row_data.get("FIRST_NAME", ""),
            last_name=row_data.get("LAST_NAME", ""),
            home_email_id=row_data.get("HOME_EMAIL_ID", ""),
            orig_hire_dt=row_data.get("ORIG_HIRE_DT", ""),
            birth_dt=row_data.get("BIRTH_DT", ""),
            is_active=row_data.get("S_EMPL_STATUS_CD") == "ACT",
        )
