from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, date, timedelta
import os
import json
import re

from google.oauth2 import service_account
from googleapiclient.discovery import build


app = FastAPI(title="Sales Handoff API", version="9.0.0-all-tasks-stable")

APP_API_KEY = os.getenv("APP_API_KEY")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
MASTER_SHEET_NAME = os.getenv("MASTER_SHEET_NAME", "Master Leads")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Master Leads layout
HEADER_ROW = 3
DATA_START_ROW = 4

# Candidate timestamp columns used for insertion review.
# The first matching header found in Master Leads will be written during append.
INSERTION_TIMESTAMP_HEADERS = [
    "Inserted At",
    "Created At",
    "Added At",
    "Entry Timestamp",
    "Data Inserted At",
    "Handover Generated",
]


# -----------------------------
# Root endpoint
# -----------------------------
@app.get("/")
def root():
    return {
        "status": "running",
        "message": "Sales Handoff API is live",
        "version": "9.0.0-all-tasks-stable",
        "mode": "Master Leads only by default. Supports insertion, daily review, weekly review, monthly review, insertion review with time, and message drafting.",
        "available_endpoints": {
            "health": "GET /",
            "docs": "GET /docs",
            "sheet_schema": "GET /sheet-schema",
            "append_leads": "POST /append-leads",
            "get_rows": "POST /get-rows",
            "review_data": "POST /get-review-data",
            "insertion_review": "POST /get-insertion-review",
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
    max_rows: Optional[int] = Field(default=50, ge=1, le=200)
    start_offset: Optional[int] = Field(default=0, ge=0)


class GetReviewDataRequest(BaseModel):
    period: str = "month"  # today | week | month
    stale_threshold_days: Optional[int] = Field(default=30, ge=1, le=365)
    max_items_per_group: Optional[int] = Field(default=8, ge=1, le=25)
    summary_only: Optional[bool] = False


class GetInsertionReviewRequest(BaseModel):
    period: str = "today"  # today | week | month
    max_items: Optional[int] = Field(default=25, ge=1, le=100)


class DraftMessageRequest(BaseModel):
    period: str = "month"  # today | week | month
    style: str = "both"  # whatsapp | email | both
    purpose: str = "review"  # review | insertion_review | all
    max_leads: Optional[int] = Field(default=8, ge=1, le=20)


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


def find_header_key(header_index: Dict[str, int], candidates: List[str]) -> Optional[str]:
    for candidate in candidates:
        key = normalize_header(candidate)
        if key in header_index:
            return key
    return None


def row_to_public_dict(row: Dict[str, Any]) -> Dict[str, Any]:
    inserted_at = first_value(row, INSERTION_TIMESTAMP_HEADERS)
    return {
        "row_number": row.get("_row_number"),
        "lead_id": first_value(row, ["Lead ID", "Lead_ID"]),
        "lead_name": first_value(row, ["Lead Name", "Lead Name ✱", "Lead_Name"]),
        "company": first_value(row, ["Company"]),
        "source": first_value(row, ["Source", "Source ✱"]),
        "owner": first_value(row, ["Owner", "Lead Owner"]),
        "stage": first_value(row, ["Stage", "Stage ✱", "Stage_Status"]),
        "last_touchpoint": first_value(row, ["Last Touchpoint", "Last Touchpoint ✱", "Last_Touchpoint_Summary"]),
        "follow_up_date": first_value(row, ["Follow-up Date", "Follow_Up_Date"]),
        "notes": first_value(row, ["Notes"]),
        "missing_fields": first_value(row, ["Missing Fields", "Missing_Fields_Declared"]),
        "handover_status": first_value(row, ["Handover Status"]),
        "outcome": first_value(row, ["Outcome"]),
        "outcome_date": first_value(row, ["Outcome Date"]),
        "reason_for_outcome": first_value(row, ["Reason for Outcome"]),
        "handover_gate": first_value(row, ["✔ Handover Gate", "Handover Gate"]),
        "feedback_alert": first_value(row, ["Feedback Alert"]),
        "inserted_at": inserted_at,
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
    period = (period or "month").lower()

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


def row_insertion_date(row: Dict[str, Any]) -> Optional[date]:
    inserted_at = first_value(row, INSERTION_TIMESTAMP_HEADERS)
    return parse_date_safe(inserted_at)


def is_in_period(row: Dict[str, Any], start_date: date, end_date: date) -> bool:
    activity_date = row_activity_date(row)
    if not activity_date:
        return False
    return start_date <= activity_date <= end_date


def is_inserted_in_period(row: Dict[str, Any], start_date: date, end_date: date) -> bool:
    inserted_date = row_insertion_date(row)
    if not inserted_date:
        return False
    return start_date <= inserted_date <= end_date


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


def breakdown(rows: List[Dict[str, Any]], field: str, top_n: int = 20) -> List[Dict[str, Any]]:
    counts: Dict[str, int] = {}
    for row in rows:
        public = row_to_public_dict(row)
        key = public.get(field) or "Unknown"
        counts[key] = counts.get(key, 0) + 1
    ordered = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:top_n]
    return [{"name": name, "count": count} for name, count in ordered]


def summarise_row(row: Dict[str, Any], classification: str) -> Dict[str, Any]:
    public = row_to_public_dict(row)
    public["sheet"] = row.get("_sheet", "")
    public["classification"] = classification
    return public


def cap_items(items: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    return items[: max(1, min(limit, 25))]


def build_review_data(
    period: str,
    stale_threshold_days: int,
    max_items_per_group: int,
    summary_only: bool,
) -> Dict[str, Any]:
    start_date, end_date = period_range(period)
    all_master_rows = rows_from_sheet(MASTER_SHEET_NAME)

    relevant_rows = [row for row in all_master_rows if is_in_period(row, start_date, end_date)]

    used_fallback = False
    if not relevant_rows:
        relevant_rows = all_master_rows
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

    summary = {
        "total_considered_count": len(relevant_rows),
        "ready_for_handoff_count": len(ready),
        "blocked_count": len(blocked),
        "stale_count": len(stale),
        "pending_feedback_count": len(pending_feedback),
        "learning_signal_count": len(learning_signals),
        "returned_per_group_limit": max_items_per_group,
        "stage_breakdown": breakdown(relevant_rows, "stage"),
        "source_breakdown": breakdown(relevant_rows, "source"),
        "owner_breakdown": breakdown(relevant_rows, "owner"),
    }

    return {
        "period": period,
        "start_date": str(start_date),
        "end_date": str(end_date),
        "used_fallback_master_leads_without_strict_period_filter": used_fallback,
        "available_tabs": [MASTER_SHEET_NAME],
        "missing_tabs": [],
        "summary": summary,
        "truncated": {
            "ready_for_handoff": len(ready) > max_items_per_group,
            "blocked": len(blocked) > max_items_per_group,
            "stale": len(stale) > max_items_per_group,
            "pending_feedback": len(pending_feedback) > max_items_per_group,
            "learning_signals": len(learning_signals) > max_items_per_group,
        },
        "ready_for_handoff": [] if summary_only else cap_items(ready, max_items_per_group),
        "blocked": [] if summary_only else cap_items(blocked, max_items_per_group),
        "stale": [] if summary_only else cap_items(stale, max_items_per_group),
        "pending_feedback": [] if summary_only else cap_items(pending_feedback, max_items_per_group),
        "learning_signals": [] if summary_only else cap_items(learning_signals, max_items_per_group),
    }


def build_insertion_review_data(period: str, max_items: int) -> Dict[str, Any]:
    start_date, end_date = period_range(period)
    all_master_rows = rows_from_sheet(MASTER_SHEET_NAME)

    timestamped_rows = [row for row in all_master_rows if row_insertion_date(row)]
    inserted_rows = [row for row in timestamped_rows if is_inserted_in_period(row, start_date, end_date)]

    items = [row_to_public_dict(row) for row in inserted_rows]
    items = sorted(items, key=lambda x: str(x.get("inserted_at", "")), reverse=True)

    return {
        "period": period,
        "start_date": str(start_date),
        "end_date": str(end_date),
        "timestamp_columns_supported": INSERTION_TIMESTAMP_HEADERS,
        "total_rows_with_insert_timestamp": len(timestamped_rows),
        "inserted_count": len(items),
        "returned_count": min(len(items), max_items),
        "truncated": len(items) > max_items,
        "inserted_leads": items[:max_items],
        "note": "Insertion review uses timestamp columns in Master Leads. If no timestamp column exists, add one such as Inserted At or Created At.",
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
    """
    Non-blocking formula helper. Failure must never break insertion.
    """
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
                body={"valueInputOption": "USER_ENTERED", "data": updates},
            ).execute()
    except Exception as exc:
        print("FORMULA COPY ERROR:", str(exc))


# -----------------------------
# Draft helpers
# -----------------------------
def format_lead_line(lead: Dict[str, Any]) -> str:
    name = lead.get("lead_name") or "Unnamed lead"
    company = lead.get("company") or "Company not specified"
    stage = lead.get("stage") or "Stage not specified"
    follow_up = lead.get("follow_up_date") or "No follow-up date"
    return f"{name} — {company} — {stage} — Follow-up: {follow_up}"


def build_whatsapp_review_draft(review_data: Dict[str, Any], max_leads: int) -> str:
    ready = review_data.get("ready_for_handoff", [])
    blocked = review_data.get("blocked", [])
    stale = review_data.get("stale", [])
    pending = review_data.get("pending_feedback", [])
    summary = review_data.get("summary", {})

    lines = [
        f"Sales Handoff Review — {review_data.get('period', '').title()}",
        f"Ready: {summary.get('ready_for_handoff_count', 0)} | Blocked: {summary.get('blocked_count', 0)} | Stale: {summary.get('stale_count', 0)} | Pending: {summary.get('pending_feedback_count', 0)}",
    ]

    if ready:
        lines.append("")
        lines.append("Ready for action:")
        for lead in ready[:max_leads]:
            lines.append(f"- {format_lead_line(lead)}")

    if blocked:
        lines.append("")
        lines.append("Blocked / missing info:")
        for lead in blocked[:min(5, max_leads)]:
            lines.append(f"- {lead.get('lead_name') or 'Unnamed lead'} — {lead.get('missing_fields') or 'Needs required field review'}")

    if stale:
        lines.append("")
        lines.append("Stale / re-engagement:")
        for lead in stale[:min(5, max_leads)]:
            lines.append(f"- {format_lead_line(lead)}")

    return "\n".join(lines)


def build_email_review_draft(review_data: Dict[str, Any], max_leads: int) -> str:
    summary = review_data.get("summary", {})
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
        f"- Total considered: {summary.get('total_considered_count', 0)}",
        f"- Ready for Handoff: {summary.get('ready_for_handoff_count', 0)}",
        f"- Blocked / Missing Information: {summary.get('blocked_count', 0)}",
        f"- Stale / Re-engagement Needed: {summary.get('stale_count', 0)}",
        f"- Awaiting Feedback: {summary.get('pending_feedback_count', 0)}",
        "",
    ]

    if ready:
        lines.append("Ready for Handoff:")
        for lead in ready[:max_leads]:
            lines.append(f"- {format_lead_line(lead)}")
            if lead.get("notes"):
                lines.append(f"  Notes: {lead.get('notes')}")
        lines.append("")

    if blocked:
        lines.append("Blocked / Missing Information:")
        for lead in blocked[:max_leads]:
            missing = lead.get("missing_fields") or "Required fields need review"
            lines.append(f"- {lead.get('lead_name') or 'Unnamed lead'} — {missing}")
        lines.append("")

    if stale:
        lines.append("Stale / Re-engagement Needed:")
        for lead in stale[:max_leads]:
            lines.append(f"- {format_lead_line(lead)}")
        lines.append("")

    if pending:
        lines.append("Awaiting Feedback:")
        for lead in pending[:max_leads]:
            lines.append(f"- {format_lead_line(lead)}")
        lines.append("")

    if review_data.get("used_fallback_master_leads_without_strict_period_filter"):
        lines.append("Note: The backend used Master Leads fallback because no strict period-matched rows were found.")
        lines.append("")

    lines.extend([
        "Recommended next action:",
        "Please prioritise ready leads first, correct blocked records, and review stale leads separately for re-engagement.",
        "",
        "Best,",
        "[Your Name]",
    ])

    return "\n".join(lines)


def build_whatsapp_insertion_draft(insertion_data: Dict[str, Any], max_leads: int) -> str:
    items = insertion_data.get("inserted_leads", [])
    lines = [
        f"Sales Sheet Insertion Review — {insertion_data.get('period', '').title()}",
        f"Inserted: {insertion_data.get('inserted_count', 0)} | Returned: {insertion_data.get('returned_count', 0)}",
    ]
    if items:
        lines.append("")
        lines.append("Inserted leads:")
        for lead in items[:max_leads]:
            inserted_at = lead.get("inserted_at") or "time not captured"
            lines.append(f"- {lead.get('lead_name') or 'Unnamed lead'} — {lead.get('company') or 'Company not specified'} — Inserted: {inserted_at}")
    else:
        lines.append("")
        lines.append("No inserted leads found for this period based on available timestamp columns.")
    return "\n".join(lines)


def build_email_insertion_draft(insertion_data: Dict[str, Any], max_leads: int) -> str:
    items = insertion_data.get("inserted_leads", [])
    lines = [
        f"Subject: Sales Sheet Insertion Review — {insertion_data.get('period', '').title()}",
        "",
        "Hi Team,",
        "",
        f"Sharing the sales sheet insertion review for {insertion_data.get('period', '')}.",
        "",
        "Summary:",
        f"- Inserted leads found: {insertion_data.get('inserted_count', 0)}",
        f"- Rows with insertion timestamp: {insertion_data.get('total_rows_with_insert_timestamp', 0)}",
        "",
    ]
    if items:
        lines.append("Inserted Leads:")
        for lead in items[:max_leads]:
            lines.append(f"- {lead.get('lead_name') or 'Unnamed lead'} — {lead.get('company') or 'Company not specified'} — Inserted: {lead.get('inserted_at') or 'time not captured'}")
        lines.append("")
    else:
        lines.append("No inserted leads were found for the selected period based on available timestamp columns.")
        lines.append("")

    lines.extend([
        "Recommended next action:",
        "Please review newly inserted records for completeness and follow-up readiness.",
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
            "Update Log",
        ],
        "header_row": HEADER_ROW,
        "data_start_row": DATA_START_ROW,
        "columns": sheet_headers,
        "insertion_timestamp_headers_supported": INSERTION_TIMESTAMP_HEADERS,
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
    warnings = []

    timestamp_key = find_header_key(header_index, INSERTION_TIMESTAMP_HEADERS)
    inserted_at_value = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    for lead in payload.leads:
        next_row_number = find_next_empty_master_row(existing_rows)
        required_updates = []
        optional_updates = []

        def add_update(update_list: List[Dict[str, Any]], normalized_header: str, value: Any):
            if normalized_header not in header_index:
                return
            col_index = header_index[normalized_header] + 1
            col_letter = col_to_letter(col_index)
            update_list.append({
                "range": f"{MASTER_SHEET_NAME}!{col_letter}{next_row_number}",
                "values": [[value if value is not None else ""]],
            })

        lead_id = ""
        if "leadid" in header_index:
            lead_id = next_lead_id(existing_rows)
            add_update(optional_updates, "leadid", lead_id)

        add_update(required_updates, "leadname", lead.lead_name)
        add_update(required_updates, "company", lead.company or "")
        add_update(required_updates, "source", normalize_source(lead.source))
        add_update(required_updates, "owner", lead.owner or "")
        add_update(required_updates, "stage", lead.stage_status or "")
        add_update(required_updates, "lasttouchpoint", lead.last_touchpoint_date or "")
        add_update(required_updates, "followupdate", lead.follow_up_date or "")
        add_update(required_updates, "notes", build_notes(lead))

        if "handoverstatus" in header_index:
            add_update(optional_updates, "handoverstatus", "Pending")

        if "missingfields" in header_index and lead.missing_fields_declared:
            add_update(optional_updates, "missingfields", lead.missing_fields_declared)

        if timestamp_key:
            add_update(optional_updates, timestamp_key, inserted_at_value)

        if not required_updates and not optional_updates:
            raise HTTPException(status_code=400, detail="No matching writable columns found in Master Leads")

        if required_updates:
            service.spreadsheets().values().batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body={"valueInputOption": "USER_ENTERED", "data": required_updates},
            ).execute()

        if optional_updates:
            try:
                service.spreadsheets().values().batchUpdate(
                    spreadsheetId=SPREADSHEET_ID,
                    body={"valueInputOption": "USER_ENTERED", "data": optional_updates},
                ).execute()
            except Exception as exc:
                warnings.append(f"Optional fields not written for row {next_row_number}: {str(exc)}")

        if next_row_number > DATA_START_ROW:
            copy_formula_cells(
                service=service,
                sheet_name=MASTER_SHEET_NAME,
                headers=sheet_headers,
                previous_row=next_row_number - 1,
                new_row=next_row_number,
            )

        try:
            inserted_row_values = get_row_values(MASTER_SHEET_NAME, next_row_number, sheet_headers)
            inserted_row = {
                header: inserted_row_values[idx] if idx < len(inserted_row_values) else ""
                for idx, header in enumerate(sheet_headers)
            }
        except Exception as exc:
            warnings.append(f"Read-back failed for row {next_row_number}: {str(exc)}")
            inserted_row = {}

        inserted_item = {
            "row_number": next_row_number,
            "lead_id": first_value(inserted_row, ["Lead ID", "Lead_ID"]) or lead_id,
            "lead_name": first_value(inserted_row, ["Lead Name", "Lead Name ✱", "Lead_Name"]) or lead.lead_name,
            "company": first_value(inserted_row, ["Company"]) or (lead.company or ""),
            "source": first_value(inserted_row, ["Source", "Source ✱"]) or normalize_source(lead.source),
            "owner": first_value(inserted_row, ["Owner", "Lead Owner"]) or (lead.owner or ""),
            "stage": first_value(inserted_row, ["Stage", "Stage ✱", "Stage_Status"]) or (lead.stage_status or ""),
            "last_touchpoint": first_value(inserted_row, ["Last Touchpoint", "Last Touchpoint ✱"]) or (lead.last_touchpoint_date or ""),
            "follow_up_date": first_value(inserted_row, ["Follow-up Date", "Follow_Up_Date"]) or (lead.follow_up_date or ""),
            "notes": first_value(inserted_row, ["Notes"]) or build_notes(lead),
            "inserted_at": first_value(inserted_row, INSERTION_TIMESTAMP_HEADERS) or (inserted_at_value if timestamp_key else ""),
        }

        inserted.append(inserted_item)

        if inserted_row:
            inserted_row["_row_number"] = next_row_number
            existing_rows.append(inserted_row)
        else:
            existing_rows.append({"_row_number": next_row_number, "Lead ID": lead_id, "Lead Name": lead.lead_name})

    return {
        "status": "success",
        "rows_added": len(inserted),
        "inserted": inserted,
        "warnings": warnings,
        "timestamp_column_used": sheet_headers[header_index[timestamp_key]] if timestamp_key else "",
        "note": "Normal insertion writes to Master Leads only. No automatic Update Log write is performed.",
    }


@app.post("/get-rows")
def get_rows(payload: GetRowsRequest, x_api_key: str = Header(...)):
    check_api_key(x_api_key)

    rows = rows_from_sheet(payload.sheet_name)
    start = payload.start_offset or 0
    max_rows = payload.max_rows or 50
    sliced = rows[start:start + max_rows]

    return {
        "sheet_name": payload.sheet_name,
        "row_count": len(rows),
        "returned_count": len(sliced),
        "start_offset": start,
        "max_rows": max_rows,
        "rows": sliced,
    }


@app.post("/get-review-data")
def get_review_data(payload: GetReviewDataRequest, x_api_key: str = Header(...)):
    check_api_key(x_api_key)

    return build_review_data(
        period=payload.period,
        stale_threshold_days=payload.stale_threshold_days or 30,
        max_items_per_group=payload.max_items_per_group or 8,
        summary_only=payload.summary_only or False,
    )


@app.post("/get-insertion-review")
def get_insertion_review(payload: GetInsertionReviewRequest, x_api_key: str = Header(...)):
    check_api_key(x_api_key)

    return build_insertion_review_data(
        period=payload.period,
        max_items=payload.max_items or 25,
    )


@app.post("/draft-message")
def draft_message(payload: DraftMessageRequest, x_api_key: str = Header(...)):
    check_api_key(x_api_key)

    max_leads = payload.max_leads or 8
    purpose = (payload.purpose or "review").lower()

    result = {
        "period": payload.period,
        "purpose": purpose,
    }

    if purpose in ["review", "all"]:
        review_data = build_review_data(
            period=payload.period,
            stale_threshold_days=30,
            max_items_per_group=max_leads,
            summary_only=False,
        )
        result["review_summary"] = review_data.get("summary", {})
        if payload.style in ["whatsapp", "both"]:
            result["whatsapp_review"] = build_whatsapp_review_draft(review_data, max_leads=max_leads)
        if payload.style in ["email", "both"]:
            result["email_review"] = build_email_review_draft(review_data, max_leads=max_leads)

    if purpose in ["insertion_review", "all"]:
        insertion_data = build_insertion_review_data(
            period=payload.period,
            max_items=max_leads,
        )
        result["insertion_summary"] = {
            "inserted_count": insertion_data.get("inserted_count", 0),
            "returned_count": insertion_data.get("returned_count", 0),
            "total_rows_with_insert_timestamp": insertion_data.get("total_rows_with_insert_timestamp", 0),
        }
        if payload.style in ["whatsapp", "both"]:
            result["whatsapp_insertion_review"] = build_whatsapp_insertion_draft(insertion_data, max_leads=max_leads)
        if payload.style in ["email", "both"]:
            result["email_insertion_review"] = build_email_insertion_draft(insertion_data, max_leads=max_leads)

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
        body={"valueInputOption": "USER_ENTERED", "data": update_data},
    ).execute()

    return {
        "status": "success",
        "updated": True,
        "lead_id": payload.lead_id,
        "row_number": target_row_number,
        "changed_fields": changed_fields,
    }
