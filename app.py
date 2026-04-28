from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime, date, timedelta
import os
import json
import re

from google.oauth2 import service_account
from googleapiclient.discovery import build

app = FastAPI(title="Sales Handoff API", version="2.0.0")

APP_API_KEY = os.getenv("APP_API_KEY")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
MASTER_SHEET_NAME = os.getenv("MASTER_SHEET_NAME", "Master Leads")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Workbook layout
HEADER_ROW = 3
DATA_START_ROW = 4


# -----------------------------
# Root endpoint
# -----------------------------
@app.get("/")
def root():
    return {"status": "API is running"}


# -----------------------------
# Google Sheets connection
# -----------------------------
def get_sheets_service():
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw:
        raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_JSON")

    info = json.loads(raw)
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=SCOPES,
    )
    return build("sheets", "v4", credentials=creds)


# -----------------------------
# API key check
# -----------------------------
def check_api_key(x_api_key: str):
    if x_api_key != APP_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


# -----------------------------
# Models
# -----------------------------
class LeadRecord(BaseModel):
    lead_name: str
    company: Optional[str] = None
    source: Optional[str] = None
    owner: Optional[str] = None
    stage_status: Optional[str] = None
    last_touchpoint_date: Optional[str] = None
    last_touchpoint_summary: Optional[str] = None
    follow_up_date: Optional[str] = None
    requirement_interest: Optional[str] = None
    notes: Optional[str] = None
    missing_fields_declared: Optional[str] = None


class AppendLeadsRequest(BaseModel):
    leads: List[LeadRecord]


class GetRowsRequest(BaseModel):
    sheet_name: str


class GetReviewDataRequest(BaseModel):
    period: str = "today"  # today | week | month
    include_tabs: Optional[List[str]] = None
    stale_threshold_days: Optional[int] = 30


class UpdateLeadRequest(BaseModel):
    lead_id: str
    updates: Dict[str, Any]


# -----------------------------
# Generic helpers
# -----------------------------
def normalize_header(text: str) -> str:
    """Normalize headers so we can match headers like 'Lead Name ✱' or '✔ Handover\\nGate'."""
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


def get_sheet_values(sheet_name: str, value_render_option: Optional[str] = None) -> List[List[str]]:
    service = get_sheets_service()
    request = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{sheet_name}!A:Z",
    )
    if value_render_option:
        request = request.clone()
        request.uri = request.uri  # no-op to keep compatibility
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{sheet_name}!A:Z",
        valueRenderOption=value_render_option or "FORMATTED_VALUE",
    ).execute()
    return result.get("values", [])


def get_headers(sheet_name: str) -> List[str]:
    values = get_sheet_values(sheet_name)
    if len(values) < HEADER_ROW:
        return []
    return values[HEADER_ROW - 1]


def get_header_index_map(headers: List[str]) -> Dict[str, int]:
    return {normalize_header(h): idx for idx, h in enumerate(headers)}


def get_row_values(sheet_name: str, row_number: int, headers: List[str], value_render_option: str = "FORMATTED_VALUE") -> List[str]:
    last_col = get_last_column_letter(headers)
    result = get_sheets_service().spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{sheet_name}!A{row_number}:{last_col}{row_number}",
        valueRenderOption=value_render_option,
    ).execute()
    values = result.get("values", [])
    return values[0] if values else []


def rows_from_sheet(sheet_name: str) -> List[Dict[str, Any]]:
    values = get_sheet_values(sheet_name)
    if len(values) < HEADER_ROW:
        return []

    headers = values[HEADER_ROW - 1]
    data_rows = values[DATA_START_ROW - 1:]

    rows = []
    for offset, r in enumerate(data_rows):
        row_dict = {}
        for i, h in enumerate(headers):
            row_dict[h] = r[i] if i < len(r) else ""
        row_dict["_sheet"] = sheet_name
        row_dict["_row_number"] = DATA_START_ROW + offset
        rows.append(row_dict)

    return rows


def first_value(row: Dict[str, Any], candidates: List[str]) -> str:
    # Try exact keys first
    for c in candidates:
        if c in row and str(row[c]).strip() != "":
            return str(row[c]).strip()

    # Then try normalized header matching
    normalized_row = {normalize_header(k): v for k, v in row.items()}
    for c in candidates:
        key = normalize_header(c)
        if key in normalized_row and str(normalized_row[key]).strip() != "":
            return str(normalized_row[key]).strip()

    return ""


def parse_date_safe(value: str) -> Optional[date]:
    if not value:
        return None

    value = str(value).strip()

    fmts = [
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

    for fmt in fmts:
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
            extracted = match.group(0)
            parsed = parse_date_safe(extracted)
            if parsed:
                return parsed

    return None


def period_range(period: str):
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
    ]

    for c in candidates:
        value = first_value(row, [c])
        parsed = parse_date_safe(value)
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

    lead_age_raw = first_value(row, ["Lead Age (Days)", "Lead_Age_Days"])
    lead_age = None
    try:
        lead_age = int(float(lead_age_raw)) if lead_age_raw != "" else None
    except Exception:
        lead_age = None

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

    if lead_age is not None and lead_age > stale_threshold_days:
        return "Stale / Re-engagement Needed"

    if "stale" in handover_status.lower():
        return "Stale / Re-engagement Needed"

    if (first_value(row, ["Handover Sent"]) and not outcome) or "awaiting feedback" in handover_status.lower():
        return "Awaiting Feedback"

    if feedback_alert and str(feedback_alert).strip() != "":
        if str(feedback_alert).strip().lower() not in ["no", "none", "0", "clear"]:
            return "Awaiting Feedback"

    if outcome or first_value(row, ["Learning Note Created"]):
        return "Learning Signal Only"

    if gate:
        if gate_positive(gate):
            return "Ready for Handoff"
        return "Blocked by Missing Fields or Gate"

    if handover_status.lower() in [
        "pending",
        "ready for handover",
        "handover sent",
        "feedback received",
        "learning generated",
    ]:
        return "Ready for Handoff"

    return "Blocked by Missing Fields or Gate"


def summarise_row(row: Dict[str, Any], classification: str) -> Dict[str, Any]:
    return {
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
        "feedback_alert": first_value(row, ["Feedback Alert"]),
        "handover_gate": first_value(row, ["✔ Handover Gate", "Handover Gate"]),
        "sheet": row.get("_sheet", ""),
        "row_number": row.get("_row_number"),
        "classification": classification,
    }


def normalize_source(source: Optional[str]) -> str:
    if not source:
        return ""
    s = source.strip()
    s_lower = s.lower()

    if s_lower.startswith("referral"):
        return "Referral"
    if "linkedin" in s_lower:
        return "LinkedIn"
    if "webinar" in s_lower:
        return "Webinar"
    if "cold email" in s_lower:
        return "Cold Email"
    if "inbound" in s_lower:
        return "Inbound Form"
    if "partner" in s_lower:
        return "Partner"
    return s


def next_lead_id(existing_rows: List[Dict[str, Any]]) -> str:
    max_num = 0
    for row in existing_rows:
        lead_id = first_value(row, ["Lead ID", "Lead_ID"])
        match = re.match(r"^L(\d+)$", lead_id.strip(), re.IGNORECASE)
        if match:
            max_num = max(max_num, int(match.group(1)))
    return f"L{max_num + 1:03d}"


def build_notes(lead: LeadRecord) -> str:
    parts: List[str] = []

    if lead.notes:
        parts.append(lead.notes.strip())

    if lead.last_touchpoint_summary:
        summary = lead.last_touchpoint_summary.strip()
        if summary and summary not in parts:
            parts.append(summary)

    if lead.requirement_interest:
        req = lead.requirement_interest.strip()
        if req and req not in parts:
            parts.append(req)

    if lead.owner:
        owner_text = f"Assigned to {lead.owner.strip()}"
        if owner_text not in parts:
            parts.append(owner_text)

    return "; ".join([p for p in parts if p])


def copy_formula_cells(service, sheet_name: str, headers: List[str], previous_row: int, new_row: int):
    """
    Copy formula-driven cells from previous row to new row if formulas exist.
    """
    formula_headers = [
        "Missing Fields",
        "Lead Age (Days)",
        "Feedback Alert",
        "✔ Handover Gate",
    ]

    header_map = get_header_index_map(headers)
    last_col = get_last_column_letter(headers)

    prev_formulas = get_row_values(sheet_name, previous_row, headers, value_render_option="FORMULA")
    if not prev_formulas:
        return

    data = []
    for header in formula_headers:
        key = normalize_header(header)
        if key not in header_map:
            continue

        idx = header_map[key]
        if idx >= len(prev_formulas):
            continue

        formula_value = prev_formulas[idx]
        if isinstance(formula_value, str) and formula_value.startswith("="):
            col_letter = col_to_letter(idx + 1)
            data.append({
                "range": f"{sheet_name}!{col_letter}{new_row}",
                "values": [[formula_value]],
            })

    if data:
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={
                "valueInputOption": "USER_ENTERED",
                "data": data,
            },
        ).execute()


# -----------------------------
# Endpoints
# -----------------------------
@app.get("/sheet-schema")
def sheet_schema(x_api_key: str = Header(...)):
    check_api_key(x_api_key)

    headers = get_headers(MASTER_SHEET_NAME)

    return {
        "spreadsheet_id": SPREADSHEET_ID,
        "tabs": ["Master Leads", "Learning Log", "Pending Feedback", "Stale Leads", "Dashboard"],
        "header_row": HEADER_ROW,
        "data_start_row": DATA_START_ROW,
        "columns": headers,
    }


@app.post("/append-leads")
def append_leads(payload: AppendLeadsRequest, x_api_key: str = Header(...)):
    check_api_key(x_api_key)

    service = get_sheets_service()
    values = get_sheet_values(MASTER_SHEET_NAME)

    if len(values) < HEADER_ROW:
        raise HTTPException(status_code=404, detail="Master sheet header row not found")

    headers = values[HEADER_ROW - 1]
    header_map = get_header_index_map(headers)
    existing_rows = rows_from_sheet(MASTER_SHEET_NAME)

    inserted = []

    for lead in payload.leads:
        row_dict = {header: "" for header in headers}

        # Lead ID
        if "leadid" in header_map:
            row_dict[headers[header_map["leadid"]]] = next_lead_id(existing_rows + [{"Lead ID": i.get("lead_id", "")} for i in inserted])

        # Core mapping
        mapping = {
            "leadname": lead.lead_name,
            "company": lead.company or "",
            "source": normalize_source(lead.source),
            "stage": lead.stage_status or "",
            "lasttouchpoint": lead.last_touchpoint_date or "",
            "followupdate": lead.follow_up_date or "",
            "notes": build_notes(lead),
        }

        for normalized_header, value in mapping.items():
            if normalized_header in header_map:
                actual_header = headers[header_map[normalized_header]]
                row_dict[actual_header] = value

        # Optional safe default
        if "handoverstatus" in header_map:
            actual_header = headers[header_map["handoverstatus"]]
            if row_dict[actual_header] == "":
                row_dict[actual_header] = "Pending"

        # If Missing Fields is NOT formula-driven in your sheet, keep this fallback
        # Otherwise formula copy from previous row will override it
        if "missingfields" in header_map and lead.missing_fields_declared:
            actual_header = headers[header_map["missingfields"]]
            row_dict[actual_header] = lead.missing_fields_declared

        next_row = DATA_START_ROW + len(existing_rows) + len(inserted)
        last_col = get_last_column_letter(headers)
        ordered_row = [row_dict.get(header, "") for header in headers]

        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{MASTER_SHEET_NAME}!A{next_row}:{last_col}{next_row}",
            valueInputOption="USER_ENTERED",
            body={"values": [ordered_row]},
        ).execute()

        # Copy formulas from previous row if applicable
        if next_row > DATA_START_ROW:
            copy_formula_cells(
                service=service,
                sheet_name=MASTER_SHEET_NAME,
                headers=headers,
                previous_row=next_row - 1,
                new_row=next_row,
            )

        # Read back inserted row for confirmation
        inserted_row_values = get_row_values(MASTER_SHEET_NAME, next_row, headers)
        inserted_row = {}
        for idx, header in enumerate(headers):
            inserted_row[header] = inserted_row_values[idx] if idx < len(inserted_row_values) else ""

        inserted.append({
            "row_number": next_row,
            "lead_id": first_value(inserted_row, ["Lead ID", "Lead_ID"]),
            "lead_name": first_value(inserted_row, ["Lead Name", "Lead Name ✱", "Lead_Name"]),
            "company": first_value(inserted_row, ["Company"]),
            "source": first_value(inserted_row, ["Source", "Source ✱"]),
            "stage": first_value(inserted_row, ["Stage", "Stage ✱", "Stage_Status"]),
            "last_touchpoint": first_value(inserted_row, ["Last Touchpoint", "Last Touchpoint ✱"]),
            "follow_up_date": first_value(inserted_row, ["Follow-up Date", "Follow_Up_Date"]),
            "notes": first_value(inserted_row, ["Notes"]),
        })

    return {
        "status": "success",
        "rows_added": len(inserted),
        "inserted": inserted,
    }


@app.post("/get-rows")
def get_rows(payload: GetRowsRequest, x_api_key: str = Header(...)):
    check_api_key(x_api_key)

    rows = rows_from_sheet(payload.sheet_name)

    return {
        "sheet_name": payload.sheet_name,
        "row_count": len(rows),
        "rows": rows,
    }


@app.post("/get-review-data")
def get_review_data(payload: GetReviewDataRequest, x_api_key: str = Header(...)):
    check_api_key(x_api_key)

    include_tabs = payload.include_tabs or [
        "Master Leads",
        "Pending Feedback",
        "Stale Leads",
        "Learning Log",
    ]
    start_date, end_date = period_range(payload.period)

    all_rows = []
    available_tabs = []
    missing_tabs = []

    for tab in include_tabs:
        try:
            tab_rows = rows_from_sheet(tab)
            available_tabs.append(tab)
            if tab_rows:
                all_rows.extend(tab_rows)
        except Exception:
            missing_tabs.append(tab)

    master_rows = [r for r in all_rows if r.get("_sheet") == "Master Leads"]

    relevant_rows = []
    for row in master_rows:
        if is_in_period(row, start_date, end_date):
            relevant_rows.append(row)

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
        classification = classify_row(
            row,
            stale_threshold_days=payload.stale_threshold_days or 30,
        )
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

    learning_log_rows = [r for r in all_rows if r.get("_sheet") == "Learning Log"]
    recent_learning = []
    for row in learning_log_rows:
        if is_in_period(row, start_date, end_date):
            recent_learning.append(row)

    return {
        "period": payload.period,
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


@app.post("/update-lead")
def update_lead(payload: UpdateLeadRequest, x_api_key: str = Header(...)):
    check_api_key(x_api_key)

    service = get_sheets_service()
    headers = get_headers(MASTER_SHEET_NAME)
    if not headers:
        raise HTTPException(status_code=404, detail="Master sheet header row not found")

    data_rows = rows_from_sheet(MASTER_SHEET_NAME)
    lead_id_key = normalize_header("Lead ID")
    header_map = get_header_index_map(headers)

    if lead_id_key not in header_map:
        raise HTTPException(status_code=400, detail="Lead ID column not found")

    target_row_number = None
    for row in data_rows:
        current_value = first_value(row, ["Lead ID", "Lead_ID"])
        if str(current_value).strip() == payload.lead_id:
            target_row_number = row["_row_number"]
            break

    if target_row_number is None:
        raise HTTPException(status_code=404, detail="Lead ID not found")

    update_data = []
    for key, value in payload.updates.items():
        # Accept exact header or normalized version
        normalized = normalize_header(key)
        if normalized not in header_map:
            continue

        col_idx = header_map[normalized] + 1
        col_letter = col_to_letter(col_idx)
        update_range = f"{MASTER_SHEET_NAME}!{col_letter}{target_row_number}"

        update_data.append({
            "range": update_range,
            "values": [[value]],
        })

    if not update_data:
        return {"status": "success", "updated": False}

    service.spreadsheets().values().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={
            "valueInputOption": "USER_ENTERED",
            "data": update_data,
        },
    ).execute()

    return {"status": "success", "updated": True}
