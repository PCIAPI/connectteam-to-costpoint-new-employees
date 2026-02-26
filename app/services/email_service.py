import os
from datetime import datetime
from typing import Any

import structlog

from app.clients.connecteam import get_team, format_date
from app.models.model import WorkforceRecord, EmployeeRecord
from app.utils.utils import save_html

logger = structlog.get_logger(__name__)

SES_FROM_EMAIL = os.environ.get("SES_FROM_EMAIL", "noreply@pci-federal.com")
SES_TO_EMAILS = os.environ.get("SES_TO_EMAILS", "")


def build_html_email(
    workforce: dict[str, WorkforceRecord],
    employees: dict[str, EmployeeRecord],
    missing_ids: list[str],
    results: list[dict[str, Any]] | None,
    dry_run: bool,
) -> str:
    run_date = datetime.now().strftime("%A, %B %d, %Y at %I:%M %p")
    total = len(missing_ids)
    projects = {workforce[eid].proj_id for eid in missing_ids}

    if dry_run:
        header_color = "#2980b9"
        header_title = "&#128269; Dry Run Preview: New Employees Pending Import"
        header_sub = "No changes have been made to Connecteam. This is a preview only."
        status_header = ""
    else:
        success_count = sum(1 for r in (results or []) if r["success"])
        fail_count = total - success_count
        header_color = "#27ae60"
        header_title = "&#10003; New Employees Imported to Connecteam"
        header_sub = (
            f"<strong>{success_count}</strong> created successfully"
            + (
                f" &nbsp;|&nbsp; <strong style='color:#e74c3c;'>{fail_count} failed</strong>"
                if fail_count
                else ""
            )
        )
        status_header = "<th style='padding:10px 12px;text-align:center;'>Status</th>"

    result_map = {r["empl_id"]: r["success"] for r in (results or [])}

    rows_html = ""
    for empl_id in missing_ids:
        emp = employees[empl_id]
        wf = workforce[empl_id]
        hire_date = format_date(emp.orig_hire_dt) if emp.orig_hire_dt else "—"
        team = get_team(wf.proj_id)

        if dry_run:
            status_cell = ""
        else:
            if result_map.get(empl_id):
                badge = '<span style="background:#27ae60;color:#fff;padding:2px 8px;border-radius:3px;font-size:11px;">&#10003; Created</span>'
            else:
                badge = '<span style="background:#e74c3c;color:#fff;padding:2px 8px;border-radius:3px;font-size:11px;">&#10007; Failed</span>'
            status_cell = f"<td style='padding:8px 12px;border-bottom:1px solid #eee;text-align:center;'>{badge}</td>"

        rows_html += f"""
        <tr>
          <td style="padding:8px 12px;border-bottom:1px solid #eee;font-family:monospace;">{emp.empl_id}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #eee;">{emp.first_name} {emp.last_name}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #eee;color:#555;">{emp.home_email_id or "—"}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #eee;font-family:monospace;">{wf.proj_id}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #eee;">{wf.proj_name}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #eee;">{team}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #eee;font-family:monospace;">{wf.bill_lab_cat_cd}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #eee;">{hire_date}</td>
          {status_cell}
        </tr>"""

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="font-family:Arial,sans-serif;font-size:14px;color:#333;margin:0;padding:0;background:#f5f5f5;">
<div style="max-width:960px;margin:20px auto;background:#fff;border-radius:6px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.1);">

  <div style="background:{header_color};padding:24px 32px;">
    <h1 style="margin:0;color:#fff;font-size:20px;">{header_title}</h1>
    <p style="margin:8px 0 0;color:rgba(255,255,255,0.85);font-size:13px;">{header_sub}</p>
  </div>

  <div style="display:flex;gap:16px;padding:24px 32px;background:#f8fffe;">
    <div style="flex:1;background:#fff;border:1px solid #d5e8d4;border-radius:4px;padding:16px;text-align:center;">
      <div style="font-size:28px;font-weight:bold;color:{header_color};">{total}</div>
      <div style="font-size:12px;color:#777;margin-top:4px;">Employees {"to Import" if dry_run else "Processed"}</div>
    </div>
    <div style="flex:1;background:#fff;border:1px solid #d5e8d4;border-radius:4px;padding:16px;text-align:center;">
      <div style="font-size:28px;font-weight:bold;color:{header_color};">{len(projects)}</div>
      <div style="font-size:12px;color:#777;margin-top:4px;">CT Projects Represented</div>
    </div>
    <div style="flex:1;background:#fff;border:1px solid #d5e8d4;border-radius:4px;padding:16px;text-align:center;">
      <div style="font-size:14px;font-weight:bold;color:#555;margin-top:6px;">{", ".join(sorted(projects))}</div>
      <div style="font-size:12px;color:#777;margin-top:4px;">Project IDs</div>
    </div>
  </div>

  <div style="padding:24px 32px;">
    <table style="width:100%;border-collapse:collapse;font-size:13px;">
      <thead>
        <tr style="background:#2c3e50;color:#fff;">
          <th style="padding:10px 12px;text-align:left;">Employee ID</th>
          <th style="padding:10px 12px;text-align:left;">Name</th>
          <th style="padding:10px 12px;text-align:left;">Email</th>
          <th style="padding:10px 12px;text-align:left;">Project ID</th>
          <th style="padding:10px 12px;text-align:left;">Project Name</th>
          <th style="padding:10px 12px;text-align:left;">Team</th>
          <th style="padding:10px 12px;text-align:left;">Labor Cat</th>
          <th style="padding:10px 12px;text-align:left;">Hire Date</th>
          {status_header}
        </tr>
      </thead>
      <tbody>{rows_html}
      </tbody>
    </table>
  </div>

  <div style="padding:16px 32px;background:#ecf0f1;font-size:11px;color:#7f8c8d;text-align:center;">
    Generated by New Employees CP &rarr; Connecteam &bull; {run_date}
    {"&nbsp;&bull;&nbsp;<strong>DRY RUN — no records were created</strong>" if dry_run else ""}
  </div>

</div>
</body>
</html>"""


def build_plain_text_email(
    workforce: dict[str, WorkforceRecord],
    employees: dict[str, EmployeeRecord],
    missing_ids: list[str],
    results: list[dict[str, Any]] | None,
    dry_run: bool,
) -> str:
    mode = "DRY RUN PREVIEW" if dry_run else "IMPORT COMPLETE"
    result_map = {r["empl_id"]: r["success"] for r in (results or [])}
    lines = [
        f"New Employees CP → Connecteam — {mode}",
        f"Total: {len(missing_ids)} employees | {len({workforce[e].proj_id for e in missing_ids})} projects",
        "",
        f"{'ID':<10} {'Name':<28} {'Project':<14} {'Team':<20} {'Labor Cat':<12} {'Hire Date':<12}"
        + ("" if dry_run else " Status"),
        "-" * (86 if dry_run else 95),
    ]
    for empl_id in missing_ids:
        emp = employees[empl_id]
        wf = workforce[empl_id]
        hire = format_date(emp.orig_hire_dt) if emp.orig_hire_dt else "—"
        status = "" if dry_run else ("Created" if result_map.get(empl_id) else "FAILED")
        lines.append(
            f"{emp.empl_id:<10} {emp.first_name + ' ' + emp.last_name:<28} {wf.proj_id:<14} "
            f"{get_team(wf.proj_id):<20} {wf.bill_lab_cat_cd:<12} {hire:<12}"
            + ("" if dry_run else f" {status}")
        )
    if dry_run:
        lines += ["", "No changes have been made. This is a preview only."]
    return "\n".join(lines)


def send_import_email(
    workforce: dict[str, WorkforceRecord],
    employees: dict[str, EmployeeRecord],
    missing_ids: list[str],
    results: list[dict[str, Any]] | None,
    dry_run: bool,
) -> bool:
    """Dry run: save HTML to S3. Live: send via AWS SES (standard region)."""
    html_body = build_html_email(workforce, employees, missing_ids, results, dry_run)
    text_body = build_plain_text_email(workforce, employees, missing_ids, results, dry_run)

    if dry_run:
        save_html(html_body, "dry_run_email_preview.html")
        logger.info("dry_run_email_saved_to_s3", file="dry_run_email_preview.html")
        return True

    recipients = [e.strip() for e in SES_TO_EMAILS.split(",") if e.strip()]
    if not recipients:
        logger.warning("no_ses_recipients_configured", env_var="SES_TO_EMAILS")
        return False

    project_ids = ", ".join(sorted({workforce[e].proj_id for e in missing_ids}))
    subject = (
        f"[Connecteam] New Employees Import Complete — "
        f"{len(missing_ids)} employees | {project_ids}"
    )

    try:
        from app.services.aws_standard import ses_client
        response = ses_client.send_email(
            Source=SES_FROM_EMAIL,
            Destination={"ToAddresses": recipients},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {
                    "Text": {"Data": text_body, "Charset": "UTF-8"},
                    "Html": {"Data": html_body, "Charset": "UTF-8"},
                },
            },
        )
        logger.info("email_sent", message_id=response.get("MessageId"), recipients=recipients)
        return True
    except Exception as exc:
        logger.error("email_send_failed", error=str(exc))
        return False
