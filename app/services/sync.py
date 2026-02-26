import os
import time
from datetime import datetime
from typing import Any

from app.clients.connecteam import ConnecteamAPIClient
from app.clients.deltek import DeltekAPIClient
from app.models.model import ConnecteamConfig, DeltekConfig, EmployeeRecord, WorkforceRecord
from app.services.email_service import send_import_email
from app.services.event_service import get_event_service, init_event_service
from app.services.secrets_service import SecretsService
from app.utils.utils import save_json


# =============================================================================
# PHASE 1: Fetch CT projects from Costpoint
# =============================================================================

def run_phase_1(deltek_client: DeltekAPIClient) -> dict[str, str]:
    """Return {proj_id: proj_name} for all active CT projects."""
    get_event_service().log_info("phase_1_starting", "phase_1_starting")

    ct_projects = deltek_client.get_ct_projects()
    save_json(ct_projects, "ne_ct_projects.json")

    get_event_service().log_info(
        "phase_1_complete",
        f"phase_1_complete count={len(ct_projects)}",
    )
    return ct_projects


# =============================================================================
# PHASE 2: Fetch workforce per project → unique employee list
# =============================================================================

def run_phase_2(
    deltek_client: DeltekAPIClient,
    ct_projects: dict[str, str],
) -> dict[str, WorkforceRecord]:
    """
    For each CT project fetch workforce data.
    Returns {empl_id: WorkforceRecord} — first project wins for duplicates.
    """
    get_event_service().log_info("phase_2_starting", "phase_2_starting")

    workforce: dict[str, WorkforceRecord] = {}
    for proj_id, proj_name in ct_projects.items():
        records = deltek_client.get_workforce(proj_id, proj_name)
        for rec in records:
            if rec.empl_id not in workforce:
                workforce[rec.empl_id] = rec
        time.sleep(0.5)

    save_json(
        [r.model_dump() for r in workforce.values()],
        "ne_workforce.json",
    )
    get_event_service().log_info(
        "phase_2_complete",
        f"phase_2_complete unique_employees={len(workforce)}",
    )
    return workforce


# =============================================================================
# PHASE 3: Fetch employee details from Costpoint (one request per EMPL_ID)
# =============================================================================

def run_phase_3(
    deltek_client: DeltekAPIClient,
    workforce: dict[str, WorkforceRecord],
) -> dict[str, EmployeeRecord]:
    """
    Fetch FIRST_NAME, LAST_NAME, HOME_EMAIL_ID, ORIG_HIRE_DT, BIRTH_DT,
    S_EMPL_STATUS_CD for each employee. Skips inactive employees.
    """
    get_event_service().log_info("phase_3_starting", "phase_3_starting")

    employees: dict[str, EmployeeRecord] = {}
    for i, empl_id in enumerate(workforce, 1):
        get_event_service().log_info(
            "fetching_employee",
            f"fetching_employee empl_id={empl_id} progress={i}/{len(workforce)}",
        )
        emp = deltek_client.get_employee(empl_id)
        if emp is None:
            get_event_service().log_error(
                "employee_not_found",
                f"employee_not_found empl_id={empl_id}",
            )
        elif not emp.is_active:
            get_event_service().log_info(
                "skipping_inactive_employee",
                f"skipping_inactive_employee empl_id={empl_id} name={emp.first_name} {emp.last_name}",
            )
        else:
            employees[empl_id] = emp
        time.sleep(0.3)

    save_json(
        [e.model_dump() for e in employees.values()],
        "ne_employees.json",
    )
    get_event_service().log_info(
        "phase_3_complete",
        f"phase_3_complete active_employees={len(employees)}",
    )
    return employees


# =============================================================================
# PHASE 4: Fetch all Connecteam users (active + archived) → existing CP IDs
# =============================================================================

def run_phase_4(ct_client: ConnecteamAPIClient) -> set[str]:
    """Return the set of EMPL_IDs already in Connecteam (any status)."""
    get_event_service().log_info("phase_4_starting", "phase_4_starting")

    existing_cp_ids = ct_client.get_existing_cp_ids()
    save_json(sorted(existing_cp_ids), "ne_existing_cp_ids.json")

    get_event_service().log_info(
        "phase_4_complete",
        f"phase_4_complete existing_count={len(existing_cp_ids)}",
    )
    return existing_cp_ids


# =============================================================================
# PHASE 5: Identify employees missing from Connecteam
# =============================================================================

def run_phase_5(
    employees: dict[str, EmployeeRecord],
    existing_cp_ids: set[str],
) -> list[str]:
    """Return list of EMPL_IDs in Costpoint workforce but not in Connecteam."""
    get_event_service().log_info("phase_5_starting", "phase_5_starting")

    missing_ids = [eid for eid in employees if eid not in existing_cp_ids]
    save_json(missing_ids, "ne_employees_to_add.json")

    get_event_service().log_info(
        "phase_5_complete",
        f"phase_5_complete missing_count={len(missing_ids)} empl_ids={missing_ids}",
    )
    return missing_ids


# =============================================================================
# PHASE 6: POST missing employees to Connecteam (or dry run)
# =============================================================================

def run_phase_6(
    ct_client: ConnecteamAPIClient,
    workforce: dict[str, WorkforceRecord],
    employees: dict[str, EmployeeRecord],
    missing_ids: list[str],
    dry_run: bool,
) -> list[dict[str, Any]]:
    """
    Dry run: build payloads and save to S3, no Connecteam writes.
    Live:    POST each missing employee one at a time.
    """
    get_event_service().log_info(
        "phase_6_starting",
        f"phase_6_starting dry_run={dry_run} count={len(missing_ids)}",
    )

    if dry_run:
        preview = [
            {
                "_empl_id": eid,
                "_name": f"{employees[eid].first_name} {employees[eid].last_name}",
                "payload": ct_client.build_payload(workforce[eid], employees[eid]),
            }
            for eid in missing_ids
        ]
        save_json(preview, "ne_dry_run_preview.json")
        get_event_service().log_info(
            "phase_6_dry_run_complete",
            f"phase_6_dry_run_complete would_create={len(preview)}",
        )
        return []

    results: list[dict[str, Any]] = []
    for i, empl_id in enumerate(missing_ids, 1):
        get_event_service().log_info(
            "creating_user",
            f"creating_user empl_id={empl_id} progress={i}/{len(missing_ids)}",
        )
        result = ct_client.post_user(workforce[empl_id], employees[empl_id])
        results.append(result)
        time.sleep(0.3)

    success_count = sum(1 for r in results if r["success"])
    save_json(results, "ne_import_results.json")

    get_event_service().log_info(
        "phase_6_complete",
        f"phase_6_complete added={success_count} failed={len(results) - success_count} total={len(results)}",
    )
    return results


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def sync(client_name: str, dry_run: bool = False) -> int:
    """
    Run the full new-employees sync pipeline.
    Returns 0 on success, 1 on failure.
    """
    function_name = os.getenv(
        "AWS_LAMBDA_FUNCTION_NAME",
        "connectteam-to-costpoint-new-employees",
    )
    init_event_service(client_name, function_name)

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    get_event_service().log_info(
        "pipeline_starting",
        f"pipeline_starting run_id={run_id} client={client_name} dry_run={dry_run}",
    )

    secrets_service = SecretsService(client_name)
    try:
        deltek_secrets = secrets_service.get_secret(f"costpoint/{client_name}")
        ct_secrets = secrets_service.get_secret(f"connectteam/{client_name}")
        deltek_config = DeltekConfig.from_env(deltek_secrets)
        ct_config = ConnecteamConfig.from_env(ct_secrets)
    except ValueError as e:
        get_event_service().log_error("configuration_error", f"configuration_error error={e}")
        return 1

    get_event_service().log_info("configurations_loaded", "configurations_loaded")

    deltek_client = DeltekAPIClient(deltek_config)
    ct_client = ConnecteamAPIClient(ct_config)

    try:
        # Phase 1: CT projects
        ct_projects = run_phase_1(deltek_client)
        if not ct_projects:
            get_event_service().log_error("no_ct_projects_found", "no_ct_projects_found")
            return 1

        # Phase 2: Workforce per project
        workforce = run_phase_2(deltek_client, ct_projects)
        if not workforce:
            get_event_service().log_error("no_workforce_records", "no_workforce_records")
            return 1

        # Phase 3: Employee details (active only)
        employees = run_phase_3(deltek_client, workforce)
        if not employees:
            get_event_service().log_info("no_active_employees_in_workforce", "no_active_employees_in_workforce")
            return 0

        # Phase 4: Existing Connecteam CP IDs
        existing_cp_ids = run_phase_4(ct_client)

        # Phase 5: Find missing employees
        missing_ids = run_phase_5(employees, existing_cp_ids)
        if not missing_ids:
            get_event_service().log_info(
                "all_ct_employees_already_in_connecteam",
                "all_ct_employees_already_in_connecteam",
            )
            return 0

        # Phase 6: POST or dry run
        results = run_phase_6(ct_client, workforce, employees, missing_ids, dry_run)

        # Email report
        send_import_email(workforce, employees, missing_ids, results or None, dry_run)

        if not dry_run:
            failed = sum(1 for r in results if not r["success"])
            if failed:
                get_event_service().log_error(
                    "pipeline_complete_with_failures",
                    f"pipeline_complete_with_failures run_id={run_id} failed={failed}",
                )
                return 1

        get_event_service().log_success(
            "pipeline_complete",
            f"pipeline_complete run_id={run_id} dry_run={dry_run}",
        )
        return 0

    except Exception as e:
        get_event_service().log_error(
            "pipeline_fatal_error",
            f"pipeline_fatal_error error={e} run_id={run_id}",
        )
        return 1

    finally:
        deltek_client.close()
        ct_client.close()
