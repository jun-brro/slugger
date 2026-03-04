"""Google Sheets integration for job tracking (best-effort)."""

from __future__ import annotations

import logging
from typing import Optional

from slugger.models import Job, SluggerConfig

logger = logging.getLogger("slugger")

HEADERS = [
    "Job ID", "Job Name", "Status", "Script", "Submit Time",
    "Start Time", "End Time", "Elapsed", "Node", "Partition",
    "GPUs", "CPUs", "Memory (MB)", "Exit Code",
]

# Characters that trigger formula interpretation in Google Sheets
_FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r", "\n")


def _sanitize_cell(val: object) -> object:
    """Prevent formula injection in Google Sheets cells.

    Prefixes dangerous first-characters with ' to force text interpretation.
    Also strips control characters that could be used to bypass sanitization.
    """
    if isinstance(val, str) and val:
        # Strip leading control characters that could mask formula prefixes
        stripped = val.lstrip("\t\r\n")
        if stripped and stripped[0] in ("=", "+", "-", "@"):
            return "'" + val
        if val[0] in _FORMULA_PREFIXES:
            return "'" + val
    return val


def _open_spreadsheet(config: SluggerConfig):
    """Open the spreadsheet by key."""
    import gspread

    gc = gspread.service_account(filename=config.google_credentials)
    return gc.open_by_key(config.spreadsheet_id)


def _get_sheet(config: SluggerConfig, project: str = ""):
    """Open worksheet tab for a project, creating it with headers if needed.

    Args:
        config: Slugger configuration.
        project: Project name used as worksheet tab title.
                 Falls back to first worksheet if empty.
    """
    import gspread

    sh = _open_spreadsheet(config)

    if not project:
        ws = sh.sheet1
    else:
        try:
            ws = sh.worksheet(project)
        except gspread.WorksheetNotFound:
            try:
                ws = sh.add_worksheet(title=project, rows=1000, cols=len(HEADERS))
                logger.info("Created new worksheet tab: %s", project)
            except gspread.exceptions.APIError:
                # Another process may have created it concurrently
                ws = sh.worksheet(project)

    # Ensure headers exist
    if ws.acell("A1").value != HEADERS[0]:
        ws.update("A1", [HEADERS])
        _apply_worksheet_formatting(ws, sh)

    return ws


def _job_to_row(job: Job) -> list:
    """Convert a Job to a spreadsheet row (sanitized against formula injection)."""
    raw = [
        job.job_id,
        job.job_name or "",
        job.status.value,
        job.script,
        job.submit_time or "",
        job.start_time or "",
        job.end_time or "",
        job.elapsed or "",
        job.node or "",
        job.partition or "",
        job.gpus if job.gpus is not None else "",
        job.cpus if job.cpus is not None else "",
        job.memory_mb if job.memory_mb is not None else "",
        job.exit_code or "",
    ]
    return [_sanitize_cell(v) for v in raw]


def create_row(job: Job, config: SluggerConfig) -> Optional[int]:
    """Append a job row to the project's worksheet. Returns row number or None."""
    if not config.sheet_configured:
        return None

    try:
        ws = _get_sheet(config, job.project)
        ws.append_row(_job_to_row(job), value_input_option="USER_ENTERED")
        row = _find_row(ws, job.job_id)
        logger.info("Created sheet row %s for job %s (tab: %s)", row, job.job_id, job.project or "default")
        return row
    except Exception as e:
        logger.warning("Failed to create sheet row for job %s: %s", job.job_id, e)
        return None


def update_row(job: Job, config: SluggerConfig) -> bool:
    """Update an existing row by job ID. Returns True on success."""
    if not config.sheet_configured:
        return False

    try:
        ws = _get_sheet(config, job.project)
        row = job.sheet_row
        # Verify cached row still points to the correct job (rows may shift)
        if row:
            cell_val = ws.acell(f"A{row}").value
            if cell_val != job.job_id:
                row = None
        if not row:
            row = _find_row(ws, job.job_id)
        if not row:
            return False

        ws.update(f"A{row}", [_job_to_row(job)], value_input_option="USER_ENTERED")
        logger.info("Updated sheet row %s for job %s", row, job.job_id)
        return True
    except Exception as e:
        logger.warning("Failed to update sheet row for job %s: %s", job.job_id, e)
        return False


def ensure_headers(config: SluggerConfig, project: str = "") -> bool:
    """Ensure the worksheet has the correct headers."""
    if not config.sheet_configured:
        return False

    try:
        _get_sheet(config, project)
        return True
    except Exception as e:
        logger.warning("Failed to ensure headers: %s", e)
        return False


def apply_formatting(config: SluggerConfig, project: str = "") -> bool:
    """Apply conditional formatting and column styling to a worksheet tab."""
    if not config.sheet_configured:
        return False

    try:
        import gspread

        sh = _open_spreadsheet(config)
        if project:
            try:
                ws = sh.worksheet(project)
            except gspread.WorksheetNotFound:
                ws = sh.sheet1
        else:
            ws = sh.sheet1

        _apply_worksheet_formatting(ws, sh)
        return True
    except Exception as e:
        logger.warning("Failed to apply formatting: %s", e)
        return False


def _build_conditional_format_rules(sheet_id: int) -> list[dict]:
    """Build conditional format rules for job status coloring."""
    STATUS_COL = 2
    STATUS_COLORS = {
        "PENDING":   {"red": 1.0,  "green": 0.85, "blue": 0.4},
        "RUNNING":   {"red": 0.3,  "green": 0.65, "blue": 1.0},
        "COMPLETED": {"red": 0.35, "green": 0.8,  "blue": 0.45},
        "FAILED":    {"red": 0.92, "green": 0.35, "blue": 0.35},
        "TIMEOUT":   {"red": 0.75, "green": 0.45, "blue": 0.85},
        "CANCELLED": {"red": 0.7,  "green": 0.7,  "blue": 0.7},
    }
    TEXT_COLORS = {
        "PENDING":   {"red": 0.55, "green": 0.45, "blue": 0.0},
        "RUNNING":   {"red": 0.0,  "green": 0.15, "blue": 0.55},
        "COMPLETED": {"red": 0.0,  "green": 0.35, "blue": 0.05},
        "FAILED":    {"red": 0.55, "green": 0.0,  "blue": 0.0},
        "TIMEOUT":   {"red": 0.35, "green": 0.1,  "blue": 0.45},
        "CANCELLED": {"red": 0.3,  "green": 0.3,  "blue": 0.3},
    }

    rules: list[dict] = []
    for status, bg_color in STATUS_COLORS.items():
        rules.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "startColumnIndex": STATUS_COL,
                        "endColumnIndex": STATUS_COL + 1,
                    }],
                    "booleanRule": {
                        "condition": {
                            "type": "TEXT_EQ",
                            "values": [{"userEnteredValue": status}],
                        },
                        "format": {
                            "backgroundColor": bg_color,
                            "textFormat": {
                                "bold": True,
                                "foregroundColor": TEXT_COLORS[status],
                            },
                        },
                    },
                },
                "index": 0,
            }
        })
    return rules


def _apply_worksheet_formatting(ws, sh) -> None:
    """Apply conditional formatting, header style, and freeze to a worksheet."""
    sheet_id = ws.id

    rules = _build_conditional_format_rules(sheet_id)

    # Header formatting
    rules.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,
                "endRowIndex": 1,
            },
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": {"red": 0.15, "green": 0.15, "blue": 0.15},
                    "textFormat": {
                        "bold": True,
                        "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
                    },
                }
            },
            "fields": "userEnteredFormat(backgroundColor,textFormat)",
        }
    })

    # Freeze header row
    rules.append({
        "updateSheetProperties": {
            "properties": {
                "sheetId": sheet_id,
                "gridProperties": {"frozenRowCount": 1},
            },
            "fields": "gridProperties.frozenRowCount",
        }
    })

    sh.batch_update({"requests": rules})
    logger.info("Applied formatting to worksheet '%s'", ws.title)


def _find_row(ws, job_id: str) -> Optional[int]:
    """Find the row number for a given job ID."""
    try:
        cell = ws.find(job_id, in_column=1)
        return cell.row if cell else None
    except Exception:
        return None
