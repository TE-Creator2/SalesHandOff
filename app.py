from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime, date, timedelta
import os
import json
import re

from google.oauth2 import service_account
from googleapiclient.discovery import build

app = FastAPI(title="Sales Handoff API", version="1.0.0")

APP_API_KEY = os.getenv("APP_API_KEY")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
MASTER_SHEET_NAME = os.getenv("MASTER_SHEET_NAME", "Master Leads")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


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
    period: str = "today"
    include_tabs: Optional[List[str]] = None
    stale_threshold_days: Optional[int] = 30


class UpdateLeadRequest(BaseModel):
    lead_id: str
    updates: Dict[str, Any]


# -----------------------------
# Helpers
# -----------------------------
def get_sheet_values(sheet_name: str) -> List[List[str]]:
    service = get_sheets_service()
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{sheet_name}!A:Z",
    ).execute()
    return result.get("values", [])


def rows_from_sheet(sheet_name: str) -> List[Dict[str, Any]]:
    values = get_sheet_values(sheet_name)
    if not values:
        return []

    headers = values[0]
    data_rows = values[1:]

    rows = []
    for r in data_rows:
        row_dict = {}
        for i, h in enumerate(headers):
            row_dict[h] = r[i] if i < len(r) else ""
        row_dict["_sheet"] = sheet_name
        rows.append(row_dict)

    return rows


def first_value(row: Dict[str, Any], candidates: List[str]) -> str:
    for c in candidates:
        if c in row and str(row[c]).strip() != "":
            return str(row[c]).strip()
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
        "Last Touchpoint Date",
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
    return value in ["ok", "yes", "y", "true", "pass", "ready", "1"]


def gate_negative(gate_value: str) -> bool:
    value = str(gate_value).strip().lower()
    return value in ["no", "n", "false", "blocked", "fail", "0"]


def classify_row(row: Dict[str, Any], stale_threshold_days: int = 30) -> str:
    lead_name = first_value(row, ["Lead Name", "Lead Name ✱", "Lead_Name"])
    source = first_value(row, ["Source", "Source ✱"])
    stage = first_value(row, ["Stage", "Stage ✱", "Stage_Status"])
    last_touchpoint = first_value(
        row,
        ["Last Touchpoint", "Last Touchpoint ✱", "Last_Touchpoint_Summary"]
    )
    missing_fields = first_value(row, ["Missing Fields", "Missing_Fields_Declared"])
    handover_status = first_value(row, ["Handover Status"])
    outcome = first_value(row, ["Outcome"])
    feedback_alert = first_value(row, ["Feedback Alert"])
    gate = first_value(row, ["✔ Handover Gate", "Handover Gate"])

    lead_age_raw = first_value(
        row,
        ["Lead Age (Days)", "Lead Age\n(Days)", "Lead_Age_Days"]
    )

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
        "last_touchpoint": first_value(
            row,
            ["Last Touchpoint", "Last Touchpoint ✱", "Last_Touchpoint_Summary"]
        ),
        "follow_up_date": first_value(row, ["Follow-up Date", "Follow_Up_Date"]),
        "notes": first_value(row, ["Notes"]),
        "missing_fields": first_value(row, ["Missing Fields", "Missing_Fields_Declared"]),
        "handover_status": first_value(row, ["Handover Status"]),
        "outcome": first_value(row, ["Outcome"]),
        "feedback_alert": first_value(row, ["Feedback Alert"]),
        "handover_gate": first_value(row, ["✔ Handover Gate", "Handover Gate"]),
        "sheet": row.get("_sheet", ""),
        "classification": classification,
    }


def col_to_letter(col_num: int) -> str:
    result = ""
    while col_num > 0:
        col_num, remainder = divmod(col_num - 1, 26)
        result = chr(65 + remainder) + result
    return result


# -----------------------------
# Endpoints
# -----------------------------
@app.get("/sheet-schema")
def sheet_schema(x_api_key: str = Header(...)):
    check_api_key(x_api_key)

    return {
        "spreadsheet_id": SPREADSHEET_ID,
        "tabs": ["Master Leads", "Learning Log", "Pending Feedback", "Stale Leads", "Dashboard"],
        "columns": [
            "Lead_ID",
            "Lead_Name",
            "Company",
            "Source",
            "Owner",
            "Stage_Status",
            "Last_Touchpoint_Date",
            "Last_Touchpoint_Summary",
            "Follow_Up_Date",
            "Requirement_Interest",
            "Notes",
            "Missing_Fields_Declared",
        ],
    }


@app.post("/append-leads")
def append_leads(payload: AppendLeadsRequest, x_api_key: str = Header(...)):
    check_api_key(x_api_key)

    service = get_sheets_service()

    values = []
    for lead in payload.leads:
        values.append([
            "",
            lead.lead_name,
            lead.company or "",
            lead.source or "",
            lead.owner or "",
            lead.stage_status or "",
            lead.last_touchpoint_date or "",
            lead.last_touchpoint_summary or "",
            lead.follow_up_date or "",
            lead.requirement_interest or "",
            lead.notes or "",
            lead.missing_fields_declared or "",
        ])

    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{MASTER_SHEET_NAME}!A:L",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": values},
    ).execute()

    return {"status": "success", "rows_added": len(values)}


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
            stale_threshold_days=payload.stale_threshold_days or 30
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
    values = get_sheet_values(MASTER_SHEET_NAME)

    if not values:
        raise HTTPException(status_code=404, detail="Master sheet is empty")

    headers = values[0]
    data_rows = values[1:]

    lead_id_col_idx = None
    for idx, header in enumerate(headers):
        if header in ["Lead ID", "Lead_ID"]:
            lead_id_col_idx = idx
            break

    if lead_id_col_idx is None:
        raise HTTPException(status_code=400, detail="Lead ID column not found")

    target_row_number = None
    for idx, row in enumerate(data_rows, start=2):
        current_value = row[lead_id_col_idx] if lead_id_col_idx < len(row) else ""
        if str(current_value).strip() == payload.lead_id:
            target_row_number = idx
            break

    if target_row_number is None:
        raise HTTPException(status_code=404, detail="Lead ID not found")

    update_data = []
    for key, value in payload.updates.items():
        if key not in headers:
            continue

        col_idx = headers.index(key) + 1
        col_letter = col_to_letter(col_idx)
        update_range = f"{MASTER_SHEET_NAME}!{col_letter}{target_row_number}"

        update_data.append({
            "range": update_range,
            "values": [[value]]
        })

    if not update_data:
        return {"status": "success", "updated": False}

    service.spreadsheets().values().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={
            "valueInputOption": "USER_ENTERED",
            "data": update_data
        }
    ).execute()

    return {"status": "success", "updated": True}
