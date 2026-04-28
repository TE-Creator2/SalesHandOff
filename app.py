from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
import os
import json
import re

from google.oauth2 import service_account
from googleapiclient.discovery import build

app = FastAPI(title="Sales Handoff API", version="4.2.0")

# ENV
APP_API_KEY = os.getenv("APP_API_KEY")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
MASTER_SHEET_NAME = os.getenv("MASTER_SHEET_NAME", "Master Leads")
LOG_SHEET_NAME = os.getenv("LOG_SHEET_NAME", "Update Log")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

HEADER_ROW = 3

LOG_HEADERS = [
    "Log ID","Timestamp","Action Type","Sheet Name","Target Row Number",
    "Lead ID","Lead Name","Changed Fields","Old Values","New Values",
    "Triggered By","Source Input Type","Status","Remarks"
]

# -----------------------------
# ROOT
# -----------------------------
@app.get("/")
def root():
    return {"status": "API is running"}

# -----------------------------
# AUTH
# -----------------------------
def check_api_key(x_api_key: str):
    if x_api_key != APP_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

# -----------------------------
# GOOGLE SERVICE
# -----------------------------
def get_service():
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw:
        raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_JSON")

    info = json.loads(raw)
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)

def get_values(sheet):
    return get_service().spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{sheet}!A:ZZ"
    ).execute().get("values", [])

# -----------------------------
# MODELS
# -----------------------------
class LeadRecord(BaseModel):
    lead_name: str
    company: Optional[str] = ""
    source: Optional[str] = ""
    owner: Optional[str] = ""
    stage_status: Optional[str] = ""
    last_touchpoint_date: Optional[str] = ""
    follow_up_date: Optional[str] = ""
    notes: Optional[str] = ""

class AppendLeadsRequest(BaseModel):
    leads: List[LeadRecord]

class GetRowsRequest(BaseModel):
    sheet_name: str

# -----------------------------
# HELPERS
# -----------------------------
def norm(x):
    return re.sub(r'[^a-z0-9]', '', str(x).lower())

def get_headers():
    vals = get_values(MASTER_SHEET_NAME)
    if len(vals) < HEADER_ROW:
        return []
    return vals[HEADER_ROW - 1]

def header_map(headers):
    return {norm(h): i for i, h in enumerate(headers)}

def next_row():
    return len(get_values(MASTER_SHEET_NAME)) + 1

def next_lead_id():
    rows = get_values(MASTER_SHEET_NAME)
    max_id = 0
    for r in rows:
        if r and str(r[0]).startswith("L"):
            try:
                max_id = max(max_id, int(r[0][1:]))
            except:
                pass
    return f"L{max_id+1:03d}"

def normalize_source(s):
    if not s:
        return ""
    s = s.lower()
    if "ref" in s:
        return "Referral"
    if "link" in s:
        return "LinkedIn"
    if "webinar" in s:
        return "Webinar"
    if "cold" in s:
        return "Cold Email"
    return s.title()

# -----------------------------
# LOGGING (SAFE)
# -----------------------------
def ensure_log_headers():
    try:
        vals = get_values(LOG_SHEET_NAME)

        if not vals or len(vals[0]) < len(LOG_HEADERS):
            get_service().spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{LOG_SHEET_NAME}!A1",
                valueInputOption="USER_ENTERED",
                body={"values": [LOG_HEADERS]},
            ).execute()
    except Exception as e:
        print("LOG HEADER ERROR:", str(e))

def log_entry(data):
    try:
        ensure_log_headers()
        get_service().spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{LOG_SHEET_NAME}!A1",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [data]},
        ).execute()
    except Exception as e:
        print("LOG ERROR:", str(e))

# -----------------------------
# APPEND LEADS
# -----------------------------
@app.post("/append-leads")
def append_leads(payload: AppendLeadsRequest, x_api_key: str = Header(...)):
    check_api_key(x_api_key)

    headers = get_headers()
    if not headers:
        raise HTTPException(status_code=404, detail="Header not found")

    hmap = header_map(headers)
    inserted = []

    for lead in payload.leads:
        row = [""] * len(headers)

        def set_val(key, val):
            k = norm(key)
            if k in hmap:
                row[hmap[k]] = val

        lead_id = next_lead_id()

        set_val("lead id", lead_id)
        set_val("lead name", lead.lead_name)
        set_val("company", lead.company)
        set_val("source", normalize_source(lead.source))
        set_val("stage", lead.stage_status)
        set_val("last touchpoint", lead.last_touchpoint_date)
        set_val("follow up date", lead.follow_up_date)
        set_val("notes", lead.notes)

        row_number = next_row()

        get_service().spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{MASTER_SHEET_NAME}!A{row_number}",
            valueInputOption="USER_ENTERED",
            body={"values": [row]},
        ).execute()

        inserted_item = {
            "row_number": row_number,
            "lead_id": lead_id,
            "lead_name": lead.lead_name
        }

        inserted.append(inserted_item)

        # SAFE LOG
        try:
            log_entry([
                f"LOG{row_number}",
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "APPEND",
                MASTER_SHEET_NAME,
                row_number,
                lead_id,
                lead.lead_name,
                "core fields",
                "NEW ROW",
                json.dumps(row),
                "GPT",
                "input",
                "SUCCESS",
                ""
            ])
        except Exception as e:
            print("LOG ERROR:", str(e))

    return {
        "status": "success",
        "rows_added": len(inserted),
        "inserted": inserted
    }

# -----------------------------
# GET ROWS
# -----------------------------
@app.post("/get-rows")
def get_rows(payload: GetRowsRequest, x_api_key: str = Header(...)):
    check_api_key(x_api_key)

    return {
        "sheet_name": payload.sheet_name,
        "rows": get_values(payload.sheet_name)
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

    target_row = None
    for row in data_rows:
        current_value = first_value(row, ["Lead ID", "Lead_ID"])
        if str(current_value).strip() == payload.lead_id:
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
        normalized = normalize_header(key)
        if normalized not in header_map:
            continue

        actual_header = headers[header_map[normalized]]
        current_old = target_row.get(actual_header, "")
        if str(current_old) == str(value):
            continue

        col_idx = header_map[normalized] + 1
        col_letter = col_to_letter(col_idx)
        update_range = f"{MASTER_SHEET_NAME}!{col_letter}{target_row_number}"

        update_data.append({
            "range": update_range,
            "values": [[value]],
        })

        changed_fields.append(actual_header)
        old_values[actual_header] = current_old
        new_values[actual_header] = value

    if not update_data:
        return {"status": "success", "updated": False}

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

    return {"status": "success", "updated": True}
