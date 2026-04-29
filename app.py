from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, date, timedelta
import os
import json
import re

from google.oauth2 import service_account
from googleapiclient.discovery import build


app = FastAPI(title="Sales Handoff API", version="6.2.0")

APP_API_KEY = os.getenv("APP_API_KEY")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
MASTER_SHEET_NAME = os.getenv("MASTER_SHEET_NAME", "Master Leads")
LOG_SHEET_NAME = os.getenv("LOG_SHEET_NAME", "Update Log")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

HEADER_ROW = 3
DATA_START_ROW = 4

LOG_HEADER_ROW = 1
LOG_DATA_START_ROW = 2

LOG_HEADERS = [
    "Log ID",
    "Timestamp",
    "Action Type",
    "Sheet Name",
    "Target Row Number",
    "Lead ID",
    "Lead Name",
    "Changed Fields",
    "Old Values",
    "New Values",
    "Triggered By",
    "Source Input Type",
    "Status",
    "Remarks",
]


# -----------------------------
# Root endpoint
# -----------------------------
@app.get("/")
def root():
    return {
        "status": "running",
        "message": "Sales Handoff API is live",
        "version": "6.2.0",
        "available_endpoints": {
            "health": "GET /",
            "docs": "GET /docs",
            "sheet_schema": "GET /sheet-schema",
            "append_leads": "POST /append-leads",
            "get_rows": "POST /get-rows",
            "review_data": "POST /get-review-data",
            "draft_message": "POST /draft-message",
            "update_lead": "POST /update-lead",
        },
    }


# -----------------------------
# Auth
# -----------------------------
def check_api_key(x_api_key: str):
    if not APP_API_KEY:
        raise HTTPException(status_code=500, detail="APP_API_KEY is not configured")

    if x_api_key != APP_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


# -----------------------------
# Google Sheets service
# -----------------------------
def get_service():
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw:
        raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_JSON")

    info = json.loads(raw)
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def get_values(sheet_name: str, value_render_option: str = "FORMATTED_VALUE") -> List[List[str]]:
    result = get_service().spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{sheet_name}!A:ZZ",
        valueRenderOption=value_render_option,
    ).execute()
    return result.get("values", [])


def ensure_sheet_exists(sheet_name: str):
    service = get_service()
    meta = service.spreadsheets().get(
        spreadsheetId=SPREADSHEET_ID,
        fields="sheets.properties.title",
    ).execute()

    existing_titles = {
        sheet["properties"]["title"]
        for sheet in meta.get("sheets", [])
        if "properties" in sheet and "title" in sheet["properties"]
    }

    if sheet_name not in existing_titles:
        service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={
                "requests": [
                    {
                        "addSheet": {
                            "properties": {
                                "title": sheet_name
                            }
                        }
                    }
                ]
            },
        ).execute()


# -----------------------------
# Models
# -----------------------------
class LeadRecord(BaseModel):
    lead_name: str
    company: Optional[str] = ""
    source: Optional[str] = ""
    owner: Optional[str] = ""
    stage_status: Optional[str] = ""
    last_touchpoint_date: Optional[str] = ""
    last_touchpoint_summary: Optional[str] = ""
    follow_up_date: Optional[str] = ""
    requirement_interest: Optional[str] = ""
    notes: Optional[str] = ""
    missing_fields_declared: Optional[str] = ""


class AppendLeadsRequest(BaseModel):
    leads: List[LeadRecord]


class GetRowsRequest(BaseModel):
    sheet_name: str


class GetReviewDataRequest(BaseModel):
    period: str = "month"  # today | week | month
    include_tabs: Optional[List[str]] = None
    stale_threshold_days: Optional[int] = 30


class DraftMessageRequest(BaseModel):
    period: str = "today"  # today | week | month
    style: str = "both"  # whatsapp | email | both


class UpdateLeadRequest(BaseModel):
    lead_id: str
    updates: Dict[str, Any]


# -----------------------------
# Header and row helpers
# -----------------------------
def normalize_header(text: str) -> str:
    if text is None:
        return ""
    text = str(text).replace("\n", " ").strip().lower()
    return "".join(re.findall(r"[a-z0-9]+", text))


def col_to_letter(col_num: int) -> str:
    result = ""
    while col_num > 0:
        col_num, remainder = divmod(col_num - 1, 26)
        result = chr(65 + remainder) + result
    return result


def get_last_column_letter(headers: List[str]) -> str:
    return col_to_letter(len(headers))


def json_compact(data: Any) -> str:
    try:
        return json.dumps(data, ensure_ascii=False)
    except Exception:
        return str(data)


def get_headers(sheet_name: str, header_row: int = HEADER_ROW) -> List[str]:
    values = get_values(sheet_name)
    if len(values) < header_row:
        return []
    return values[header_row - 1]


def get_header_index_map(headers: List[str]) -> Dict[str, int]:
    return {normalize_header(header): idx for idx, header in enumerate(headers)}


def get_row_values(
    sheet_name: str,
    row_number: int,
    headers: List[str],
    value_render_option: str = "FORMATTED_VALUE",
) -> List[str]:
    last_col = get_last_column_letter(headers)
    result = get_service().spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{sheet_name}!A{row_number}:{last_col}{row_number}",
        valueRenderOption=value_render_option,
    ).execute()

    values = result.get("values", [])
    return values[0] if values else []


def rows_from_sheet_generic(sheet_name: str, header_row: int, data_start_row: int) -> List[Dict[str, Any]]:
    values = get_values(sheet_name)
    if len(values) < header_row:
        return []

    headers = values[header_row - 1]
    data_rows = values[data_start_row - 1:]

    rows = []
    for offset, raw_row in enumerate(data_rows):
        row_dict = {}
        for index, header in enumerate(headers):
            row_dict[header] = raw_row[index] if index < len(raw_row) else ""

        row_dict["_sheet"] = sheet_name
        row_dict["_row_number"] = data_start_row + offset
        rows.append(row_dict)

    return rows


def rows_from_sheet(sheet_name: str) -> List[Dict[str, Any]]:
    return rows_from_sheet_generic(sheet_name, HEADER_ROW, DATA_START_ROW)


def rows_from_log_sheet() -> List[Dict[str, Any]]:
    ensure_log_sheet_headers()
    return rows_from_sheet_generic(LOG_SHEET_NAME, LOG_HEADER_ROW, LOG_DATA_START_ROW)


def first_value(row: Dict[str, Any], candidates: List[str]) -> str:
    for candidate in candidates:
        if candidate in row and str(row[candidate]).strip() != "":
            return str(row[candidate]).strip()

    normalized_row = {normalize_header(key): value for key, value in row.items()}

    for candidate in candidates:
        normalized_candidate = normalize_header(candidate)
        if normalized_candidate in normalized_row and str(normalized_row[normalized_candidate]).strip() != "":
            return str(normalized_row[normalized_candidate]).strip()

    return ""


def row_to_public_dict(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "row_number": row.get("_row_number"),
        "lead_id": first_value(row, ["Lead ID", "Lead_ID"]),
        "lead_name": first_value(row, ["Lead Name", "Lead Name ✱", "Lead_Name"]),
        "company": first_value(row, ["Company"]),
        "source": first_value(row, ["Source", "Source ✱"]),
        "stage": first_value(row, ["Stage", "Stage ✱", "Stage_Status"]),
        "last_touchpoint": first_value(row, ["Last Touchpoint", "Last Touchpoint ✱", "Last_Touchpoint_Summary"]),
        "follow_up_date": first_value(row, ["Follow-up Date", "Follow_Up_Date"]),
        "notes": first_value(row, ["Notes"]),
        "missing_fields": first_value(row, ["Missing Fields", "Missing_Fields_Declared"]),
        "handover_status": first_value(row, ["Handover Status"]),
        "outcome": first_value(row, ["Outcome"]),
        "reason_for_outcome": first_value(row, ["Reason for Outcome"]),
    }


# -----------------------------
# Date and review helpers
# -----------------------------
def parse_date_safe(value: str) -> Optional[date]:
    if not value:
        return None

    value = str(value).strip()

    formats = [
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%d-%b-%y",
        "%d-%b-%Y",
        "%d %b %Y",
        "%d %B %Y",
        "%Y/%m/%d",
        "%d-%m-%y",
        "%d/%m/%y",
        "%Y-%m-%d %H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(value, fmt).date()
        except Exception:
            pass

    current_year = date.today().year
    for fmt in ["%d %B %Y", "%d %b %Y"]:
        try:
            return datetime.strptime(f"{value} {current_year}", fmt).date()
        except Exception:
            pass

    patterns = [
        r"\b\d{4}-\d{2}-\d{2}\b",
        r"\b\d{1,2}/\d{1,2}/\d{4}\b",
        r"\b\d{1,2}-\d{1,2}-\d{4}\b",
        r"\b\d{1,2} [A-Za-z]+ \d{4}\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, value)
        if match:
            parsed = parse_date_safe(match.group(0))
            if parsed:
                return parsed

    return None


def period_range(period: str) -> Tuple[date, date]:
    today = date.today()

    if period == "today":
        return today, today

    if period == "week":
        start = today - timedelta(days=today.weekday())
        end = start + timedelta(days=6)
        return start, end

    if period == "month":
        start = today.replace(day=1)
        if start.month == 12:
            next_month = date(start.year + 1, 1, 1)
        else:
            next_month = date(start.year, start.month + 1, 1)
        end = next_month - timedelta(days=1)
        return start, end

    return today, today


def row_activity_date(row: Dict[str, Any]) -> Optional[date]:
    candidates = [
        "Handover Generated",
        "Handover Sent",
        "Feedback Entered",
        "Learning Note Created",
        "Outcome Date",
        "Follow-up Date",
        "Last Touchpoint",
        "Last Touchpoint ✱",
    ]

    for candidate in candidates:
        parsed = parse_date_safe(first_value(row, [candidate]))
        if parsed:
            return parsed

    return None


def is_in_period(row: Dict[str, Any], start_date: date, end_date: date) -> bool:
    activity_date = row_activity_date(row)
    if not activity_date:
        return False
    return start_date <= activity_date <= end_date


def gate_positive(gate_value: str) -> bool:
    value = str(gate_value).strip().lower()
    return value in ["ok", "yes", "y", "true", "pass", "ready", "1", "✔ complete", "complete"]


def gate_negative(gate_value: str) -> bool:
    value = str(gate_value).strip().lower()
    return value in ["no", "n", "false", "blocked", "fail", "0"]


def classify_row(row: Dict[str, Any], stale_threshold_days: int = 30) -> str:
    lead_name = first_value(row, ["Lead Name", "Lead Name ✱", "Lead_Name"])
    source = first_value(row, ["Source", "Source ✱"])
    stage = first_value(row, ["Stage", "Stage ✱", "Stage_Status"])
    last_touchpoint = first_value(row, ["Last Touchpoint", "Last Touchpoint ✱", "Last_Touchpoint_Summary"])
    missing_fields = first_value(row, ["Missing Fields", "Missing_Fields_Declared"])
    handover_status = first_value(row, ["Handover Status"])
    outcome = first_value(row, ["Outcome"])
    feedback_alert = first_value(row, ["Feedback Alert"])
    gate = first_value(row, ["✔ Handover Gate", "Handover Gate"])

    required_missing = []
    if not lead_name:
        required_missing.append("Lead Name")
    if not source:
        required_missing.append("Source")
    if not stage:
        required_missing.append("Stage")
    if not last_touchpoint:
        required_missing.append("Last Touchpoint")

    if required_missing:
        return "Blocked by Missing Fields or Gate"

    if gate_negative(gate):
        return "Blocked by Missing Fields or Gate"

    if missing_fields and gate and not gate_positive(gate):
        return "Blocked by Missing Fields or Gate"

    lead_age = None
    lead_age_raw = first_value(row, ["Lead Age (Days)", "Lead_Age_Days"])
    try:
        lead_age = int(float(lead_age_raw)) if lead_age_raw != "" else None
    except Exception:
        lead_age = None

    if lead_age is None:
        touchpoint_date = parse_date_safe(last_touchpoint)
        if touchpoint_date:
            lead_age = (date.today() - touchpoint_date).days

    if lead_age is not None and lead_age > stale_threshold_days:
        return "Stale / Re-engagement Needed"

    if "stale" in handover_status.lower():
        return "Stale / Re-engagement Needed"

    if (first_value(row, ["Handover Sent"]) and not outcome) or "awaiting feedback" in handover_status.lower():
        return "Awaiting Feedback"

    if feedback_alert and feedback_alert.strip().lower() not in ["no", "none", "0", "clear"]:
        return "Awaiting Feedback"

    if outcome or first_value(row, ["Learning Note Created"]):
        return "Learning Signal Only"

    if gate and gate_positive(gate):
        return "Ready for Handoff"

    if handover_status.lower() in [
        "pending",
        "ready for handover",
        "handover sent",
        "feedback received",
        "learning generated",
    ]:
        return "Ready for Handoff"

    return "Ready for Handoff"


def summarise_row(row: Dict[str, Any], classification: str) -> Dict[str, Any]:
    public = row_to_public_dict(row)
    public["sheet"] = row.get("_sheet", "")
    public["classification"] = classification
    public["handover_gate"] = first_value(row, ["✔ Handover Gate", "Handover Gate"])
    public["feedback_alert"] = first_value(row, ["Feedback Alert"])
    return public


def build_review_data(period: str, include_tabs: Optional[List[str]], stale_threshold_days: int) -> Dict[str, Any]:
    include_tabs = include_tabs or [
        MASTER_SHEET_NAME,
        "Pending Feedback",
        "Stale Leads",
        "Learning Log",
    ]

    start_date, end_date = period_range(period)

    all_rows = []
    available_tabs = []
    missing_tabs = []

    for tab in include_tabs:
        try:
            if tab == LOG_SHEET_NAME:
                tab_rows = rows_from_log_sheet()
            else:
                tab_rows = rows_from_sheet(tab)

            available_tabs.append(tab)
            all_rows.extend(tab_rows)
        except Exception:
            missing_tabs.append(tab)

    master_rows = [row for row in all_rows if row.get("_sheet") == MASTER_SHEET_NAME]

    relevant_rows = [
        row for row in master_rows
        if is_in_period(row, start_date, end_date)
    ]

    used_fallback = False
    if not relevant_rows and master_rows:
        relevant_rows = master_rows
        used_fallback = True

    ready = []
    blocked = []
    stale = []
    pending_feedback = []
    learning_signals = []

    for row in relevant_rows:
        classification = classify_row(row, stale_threshold_days=stale_threshold_days)
        item = summarise_row(row, classification)

        if classification == "Ready for Handoff":
            ready.append(item)
        elif classification == "Blocked by Missing Fields or Gate":
            blocked.append(item)
        elif classification == "Stale / Re-engagement Needed":
            stale.append(item)
        elif classification == "Awaiting Feedback":
            pending_feedback.append(item)
        elif classification == "Learning Signal Only":
            learning_signals.append(item)

    learning_log_rows = [row for row in all_rows if row.get("_sheet") == "Learning Log"]
    recent_learning = [
        row for row in learning_log_rows
        if is_in_period(row, start_date, end_date)
    ]

    return {
        "period": period,
        "start_date": str(start_date),
        "end_date": str(end_date),
        "used_fallback_master_leads_without_strict_period_filter": used_fallback,
        "available_tabs": available_tabs,
        "missing_tabs": missing_tabs,
        "summary": {
            "ready_for_handoff_count": len(ready),
            "blocked_count": len(blocked),
            "stale_count": len(stale),
            "pending_feedback_count": len(pending_feedback),
            "learning_signal_count": len(learning_signals),
            "recent_learning_log_count": len(recent_learning),
        },
        "ready_for_handoff": ready,
        "blocked": blocked,
        "stale": stale,
        "pending_feedback": pending_feedback,
        "learning_signals": learning_signals,
        "recent_learning_log_rows": recent_learning,
    }


# -----------------------------
# Business helpers
# -----------------------------
def normalize_source(source: Optional[str]) -> str:
    if not source:
        return ""

    source_text = source.strip()
    source_lower = source_text.lower()

    if source_lower.startswith("referral") or "referred" in source_lower:
        return "Referral"
    if "linkedin" in source_lower or "linked in" in source_lower:
        return "LinkedIn"
    if "webinar" in source_lower:
        return "Webinar"
    if "cold email" in source_lower or "cold" in source_lower:
        return "Cold Email"
    if "inbound" in source_lower:
        return "Inbound Form"
    if "partner" in source_lower:
        return "Partner"

    return source_text


def next_lead_id(existing_rows: List[Dict[str, Any]]) -> str:
    max_number = 0

    for row in existing_rows:
        lead_id = first_value(row, ["Lead ID", "Lead_ID"])
        match = re.match(r"^L(\d+)$", lead_id.strip(), re.IGNORECASE)
        if match:
            max_number = max(max_number, int(match.group(1)))

    return f"L{max_number + 1:03d}"


def find_next_empty_master_row(existing_rows: List[Dict[str, Any]]) -> int:
    last_occupied_row = DATA_START_ROW - 1

    for row in existing_rows:
        lead_id = first_value(row, ["Lead ID", "Lead_ID"])
        lead_name = first_value(row, ["Lead Name", "Lead Name ✱", "Lead_Name"])

        if lead_id or lead_name:
            row_number = row.get("_row_number")
            if row_number and row_number > last_occupied_row:
                last_occupied_row = row_number

    return last_occupied_row + 1


def build_notes(lead: LeadRecord) -> str:
    parts = []

    if lead.notes:
        parts.append(lead.notes.strip())

    if lead.last_touchpoint_summary:
        summary = lead.last_touchpoint_summary.strip()
        if summary and summary not in parts:
            parts.append(summary)

    if lead.requirement_interest:
        requirement = lead.requirement_interest.strip()
        if requirement and requirement not in parts:
            parts.append(requirement)

    if lead.owner:
        owner_text = f"Assigned to {lead.owner.strip()}"
        if owner_text not in parts:
            parts.append(owner_text)

    return "; ".join([part for part in parts if part])


def copy_formula_cells(service, sheet_name: str, headers: List[str], previous_row: int, new_row: int):
    try:
        formula_headers = [
            "Missing Fields",
            "Lead Age (Days)",
            "Feedback Alert",
            "✔ Handover Gate",
        ]

        header_index = get_header_index_map(headers)
        previous_formulas = get_row_values(sheet_name, previous_row, headers, value_render_option="FORMULA")

        if not previous_formulas:
            return

        updates = []

        for header in formula_headers:
            key = normalize_header(header)
            if key not in header_index:
                continue

            index = header_index[key]
            if index >= len(previous_formulas):
                continue

            formula_value = previous_formulas[index]

            if isinstance(formula_value, str) and formula_value.startswith("="):
                col_letter = col_to_letter(index + 1)
                updates.append({
                    "range": f"{sheet_name}!{col_letter}{new_row}",
                    "values": [[formula_value]],
                })

        if updates:
            service.spreadsheets().values().batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body={
                    "valueInputOption": "USER_ENTERED",
                    "data": updates,
                },
            ).execute()
    except Exception as exc:
        print("FORMULA COPY ERROR:", str(exc))


# -----------------------------
# Update Log helpers
# -----------------------------
def ensure_log_sheet_headers():
    try:
        ensure_sheet_exists(LOG_SHEET_NAME)
        values = get_values(LOG_SHEET_NAME)

        if not values or len(values[0]) < len(LOG_HEADERS):
            get_service().spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{LOG_SHEET_NAME}!A1",
                valueInputOption="USER_ENTERED",
                body={"values": [LOG_HEADERS]},
            ).execute()
    except Exception as exc:
        print("LOG HEADER ERROR:", str(exc))


def next_log_id() -> str:
    try:
        log_rows = rows_from_sheet_generic(LOG_SHEET_NAME, LOG_HEADER_ROW, LOG_DATA_START_ROW)
    except Exception:
        log_rows = []

    max_number = 0

    for row in log_rows:
        log_id = first_value(row, ["Log ID"])
        match = re.match(r"^LOG(\d+)$", log_id.strip(), re.IGNORECASE)
        if match:
            max_number = max(max_number, int(match.group(1)))

    return f"LOG{max_number + 1:03d}"


def append_log_entry(
    action_type: str,
    sheet_name: str,
    target_row_number: Any,
    lead_id: str,
    lead_name: str,
    changed_fields: Any,
    old_values: Any,
    new_values: Any,
    triggered_by: str = "GPT Action",
    source_input_type: str = "mixed input",
    status: str = "SUCCESS",
    remarks: str = "",
):
    try:
        ensure_log_sheet_headers()

        row = [
            next_log_id(),
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            action_type,
            sheet_name,
            str(target_row_number) if target_row_number is not None else "",
            lead_id or "",
            lead_name or "",
            changed_fields if isinstance(changed_fields, str) else json_compact(changed_fields),
            old_values if isinstance(old_values, str) else json_compact(old_values),
            new_values if isinstance(new_values, str) else json_compact(new_values),
            triggered_by,
            source_input_type,
            status,
            remarks,
        ]

        get_service().spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{LOG_SHEET_NAME}!A1",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [row]},
        ).execute()
    except Exception as exc:
        print("LOG ERROR:", str(exc))


# -----------------------------
# Draft helpers
# -----------------------------
def format_lead_line(lead: Dict[str, Any]) -> str:
    name = lead.get("lead_name") or "Unnamed lead"
    company = lead.get("company") or "Company not specified"
    stage = lead.get("stage") or "Stage not specified"
    follow_up = lead.get("follow_up_date") or "No follow-up date"
    return f"{name} — {company} — {stage} — Follow-up: {follow_up}"


def build_whatsapp_draft(review_data: Dict[str, Any]) -> str:
    ready = review_data.get("ready_for_handoff", [])
    blocked = review_data.get("blocked", [])
    stale = review_data.get("stale", [])
    pending = review_data.get("pending_feedback", [])

    lines = [
        f"Sales Handoff Review — {review_data.get('period', '').title()}",
        "",
        f"Ready: {len(ready)} | Blocked: {len(blocked)} | Stale: {len(stale)} | Pending Feedback: {len(pending)}",
    ]

    if ready:
        lines.append("")
        lines.append("Ready for action:")
        for lead in ready[:8]:
            lines.append(f"- {format_lead_line(lead)}")

    if blocked:
        lines.append("")
        lines.append("Blocked / missing info:")
        for lead in blocked[:5]:
            missing = lead.get("missing_fields") or "Required fields need review"
            lines.append(f"- {lead.get('lead_name') or 'Unnamed lead'} — {missing}")

    if stale:
        lines.append("")
        lines.append("Stale / re-engagement:")
        for lead in stale[:5]:
            lines.append(f"- {format_lead_line(lead)}")

    return "\n".join(lines)


def build_email_draft(review_data: Dict[str, Any]) -> str:
    ready = review_data.get("ready_for_handoff", [])
    blocked = review_data.get("blocked", [])
    stale = review_data.get("stale", [])
    pending = review_data.get("pending_feedback", [])

    subject = f"Sales Handoff Review — {review_data.get('period', '').title()}"

    lines = [
        f"Subject: {subject}",
        "",
        "Hi Team,",
        "",
        f"Sharing the {review_data.get('period', '')} sales handoff review.",
        "",
        "Summary:",
        f"- Ready for Handoff: {len(ready)}",
        f"- Blocked / Missing Information: {len(blocked)}",
        f"- Stale / Re-engagement Needed: {len(stale)}",
        f"- Awaiting Feedback: {len(pending)}",
        "",
    ]

    if ready:
        lines.append("Ready for Handoff:")
        for lead in ready:
            lines.append(f"- {format_lead_line(lead)}")
            if lead.get("notes"):
                lines.append(f"  Notes: {lead.get('notes')}")
        lines.append("")

    if blocked:
        lines.append("Blocked / Missing Information:")
        for lead in blocked:
            missing = lead.get("missing_fields") or "Required fields need review"
            lines.append(f"- {lead.get('lead_name') or 'Unnamed lead'} — {missing}")
        lines.append("")

    if stale:
        lines.append("Stale / Re-engagement Needed:")
        for lead in stale:
            lines.append(f"- {format_lead_line(lead)}")
        lines.append("")

    if pending:
        lines.append("Awaiting Feedback:")
        for lead in pending:
            lines.append(f"- {format_lead_line(lead)}")
        lines.append("")

    lines.extend([
        "Recommended next action:",
        "Please prioritise ready leads first, correct blocked records, and review stale leads separately for re-engagement.",
        "",
        "Best,",
        "[Your Name]",
    ])

    return "\n".join(lines)


# -----------------------------
# Endpoints
# -----------------------------
@app.get("/sheet-schema")
def sheet_schema(x_api_key: str = Header(...)):
    check_api_key(x_api_key)

    sheet_headers = get_headers(MASTER_SHEET_NAME)

    return {
        "spreadsheet_id": SPREADSHEET_ID,
        "tabs": [
            MASTER_SHEET_NAME,
            "Learning Log",
            "Pending Feedback",
            "Stale Leads",
            "Dashboard",
            LOG_SHEET_NAME,
        ],
        "header_row": HEADER_ROW,
        "data_start_row": DATA_START_ROW,
        "columns": sheet_headers,
    }


@app.post("/append-leads")
def append_leads(payload: AppendLeadsRequest, x_api_key: str = Header(...)):
    check_api_key(x_api_key)

    service = get_service()
    values = get_values(MASTER_SHEET_NAME)

    if len(values) < HEADER_ROW:
        raise HTTPException(status_code=404, detail="Master sheet header row not found")

    sheet_headers = values[HEADER_ROW - 1]
    header_index = get_header_index_map(sheet_headers)
    existing_rows = rows_from_sheet(MASTER_SHEET_NAME)

    inserted = []

    for lead in payload.leads:
        next_row_number = find_next_empty_master_row(existing_rows)
        row_updates = []
        inserted_item = {
            "row_number": next_row_number,
            "lead_id": "",
            "lead_name": lead.lead_name,
            "company": lead.company or "",
            "source": normalize_source(lead.source),
            "stage": lead.stage_status or "",
            "last_touchpoint": lead.last_touchpoint_date or "",
            "follow_up_date": lead.follow_up_date or "",
            "notes": build_notes(lead),
        }

        def add_cell_update(normalized_header: str, value: Any):
            if normalized_header not in header_index:
                return

            col_index = header_index[normalized_header] + 1
            col_letter = col_to_letter(col_index)
            row_updates.append({
                "range": f"{MASTER_SHEET_NAME}!{col_letter}{next_row_number}",
                "values": [[value if value is not None else ""]],
            })

        lead_id = ""
        if "leadid" in header_index:
            lead_id = next_lead_id(existing_rows)
            inserted_item["lead_id"] = lead_id
            add_cell_update("leadid", lead_id)

        add_cell_update("leadname", lead.lead_name)
        add_cell_update("company", lead.company or "")
        add_cell_update("source", normalize_source(lead.source))
        add_cell_update("stage", lead.stage_status or "")
        add_cell_update("lasttouchpoint", lead.last_touchpoint_date or "")
        add_cell_update("followupdate", lead.follow_up_date or "")
        add_cell_update("notes", build_notes(lead))

        if "handoverstatus" in header_index:
            add_cell_update("handoverstatus", "Pending")

        if "missingfields" in header_index and lead.missing_fields_declared:
            add_cell_update("missingfields", lead.missing_fields_declared)

        if not row_updates:
            raise HTTPException(status_code=400, detail="No matching writable columns found in Master Leads")

        service.spreadsheets().values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={
                "valueInputOption": "USER_ENTERED",
                "data": row_updates,
            },
        ).execute()

        if next_row_number > DATA_START_ROW:
            copy_formula_cells(
                service=service,
                sheet_name=MASTER_SHEET_NAME,
                headers=sheet_headers,
                previous_row=next_row_number - 1,
                new_row=next_row_number,
            )

        inserted_row_values = get_row_values(MASTER_SHEET_NAME, next_row_number, sheet_headers)
        inserted_row = {}

        for idx, header in enumerate(sheet_headers):
            inserted_row[header] = inserted_row_values[idx] if idx < len(inserted_row_values) else ""

        inserted_item = {
            "row_number": next_row_number,
            "lead_id": first_value(inserted_row, ["Lead ID", "Lead_ID"]) or lead_id,
            "lead_name": first_value(inserted_row, ["Lead Name", "Lead Name ✱", "Lead_Name"]) or lead.lead_name,
            "company": first_value(inserted_row, ["Company"]) or (lead.company or ""),
            "source": first_value(inserted_row, ["Source", "Source ✱"]) or normalize_source(lead.source),
            "stage": first_value(inserted_row, ["Stage", "Stage ✱", "Stage_Status"]) or (lead.stage_status or ""),
            "last_touchpoint": first_value(inserted_row, ["Last Touchpoint", "Last Touchpoint ✱"]) or (lead.last_touchpoint_date or ""),
            "follow_up_date": first_value(inserted_row, ["Follow-up Date", "Follow_Up_Date"]) or (lead.follow_up_date or ""),
            "notes": first_value(inserted_row, ["Notes"]) or build_notes(lead),
        }

        inserted.append(inserted_item)
        inserted_row["_row_number"] = next_row_number
        existing_rows.append(inserted_row)

        append_log_entry(
            action_type="APPEND",
            sheet_name=MASTER_SHEET_NAME,
            target_row_number=next_row_number,
            lead_id=inserted_item["lead_id"],
            lead_name=inserted_item["lead_name"],
            changed_fields=["Lead ID", "Lead Name", "Company", "Source", "Stage", "Last Touchpoint", "Follow-up Date", "Notes"],
            old_values="NEW ROW",
            new_values=inserted_item,
            triggered_by="GPT Action",
            source_input_type="mixed input",
            status="SUCCESS",
            remarks="Lead appended to Master Leads",
        )

    return {
        "status": "success",
        "rows_added": len(inserted),
        "inserted": inserted,
    }


@app.post("/get-rows")
def get_rows(payload: GetRowsRequest, x_api_key: str = Header(...)):
    check_api_key(x_api_key)

    if payload.sheet_name == LOG_SHEET_NAME:
        rows = rows_from_log_sheet()
    else:
        rows = rows_from_sheet(payload.sheet_name)

    return {
        "sheet_name": payload.sheet_name,
        "row_count": len(rows),
        "rows": rows,
    }


@app.post("/get-review-data")
def get_review_data(payload: GetReviewDataRequest, x_api_key: str = Header(...)):
    check_api_key(x_api_key)

    return build_review_data(
        period=payload.period,
        include_tabs=payload.include_tabs,
        stale_threshold_days=payload.stale_threshold_days or 30,
    )


@app.post("/draft-message")
def draft_message(payload: DraftMessageRequest, x_api_key: str = Header(...)):
    check_api_key(x_api_key)

    review_data = build_review_data(
        period=payload.period,
        include_tabs=None,
        stale_threshold_days=30,
    )

    result = {
        "period": payload.period,
        "summary": review_data.get("summary", {}),
    }

    if payload.style in ["whatsapp", "both"]:
        result["whatsapp"] = build_whatsapp_draft(review_data)

    if payload.style in ["email", "both"]:
        result["email"] = build_email_draft(review_data)

    return result


@app.post("/update-lead")
def update_lead(payload: UpdateLeadRequest, x_api_key: str = Header(...)):
    check_api_key(x_api_key)

    service = get_service()
    sheet_headers = get_headers(MASTER_SHEET_NAME)

    if not sheet_headers:
        raise HTTPException(status_code=404, detail="Master sheet header row not found")

    rows = rows_from_sheet(MASTER_SHEET_NAME)
    header_index = get_header_index_map(sheet_headers)

    if "leadid" not in header_index:
        raise HTTPException(status_code=400, detail="Lead ID column not found")

    target_row = None

    for row in rows:
        current_lead_id = first_value(row, ["Lead ID", "Lead_ID"])
        if current_lead_id.strip() == payload.lead_id:
            target_row = row
            break

    if target_row is None:
        raise HTTPException(status_code=404, detail="Lead ID not found")

    target_row_number = target_row["_row_number"]

    old_values = {}
    new_values = {}
    changed_fields = []
    update_data = []

    for key, value in payload.updates.items():
        normalized_key = normalize_header(key)

        if normalized_key not in header_index:
            continue

        actual_header = sheet_headers[header_index[normalized_key]]
        current_value = target_row.get(actual_header, "")

        if str(current_value) == str(value):
            continue

        col_index = header_index[normalized_key] + 1
        col_letter = col_to_letter(col_index)

        update_data.append({
            "range": f"{MASTER_SHEET_NAME}!{col_letter}{target_row_number}",
            "values": [[value]],
        })

        changed_fields.append(actual_header)
        old_values[actual_header] = current_value
        new_values[actual_header] = value

    if not update_data:
        return {
            "status": "success",
            "updated": False,
            "reason": "No matching changed fields found",
        }

    service.spreadsheets().values().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={
            "valueInputOption": "USER_ENTERED",
            "data": update_data,
        },
    ).execute()

    append_log_entry(
        action_type="UPDATE",
        sheet_name=MASTER_SHEET_NAME,
        target_row_number=target_row_number,
        lead_id=payload.lead_id,
        lead_name=first_value(target_row, ["Lead Name", "Lead Name ✱", "Lead_Name"]),
        changed_fields=changed_fields,
        old_values=old_values,
        new_values=new_values,
        triggered_by="GPT Action",
        source_input_type="sheet update",
        status="SUCCESS",
        remarks="Existing lead updated",
    )

    return {
        "status": "success",
        "updated": True,
        "lead_id": payload.lead_id,
        "row_number": target_row_number,
        "changed_fields": changed_fields,
    }
