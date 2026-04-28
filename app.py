from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime, date, timedelta
import os
import json
import re

from google.oauth2 import service_account
from googleapiclient.discovery import build

app = FastAPI(title="Sales Handoff API", version="4.1.0")

APP_API_KEY = os.getenv("APP_API_KEY")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
MASTER_SHEET_NAME = os.getenv("MASTER_SHEET_NAME", "Master Leads")
LOG_SHEET_NAME = os.getenv("LOG_SHEET_NAME", "Update Log")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

HEADER_ROW = 3
DATA_START_ROW = 4


# -----------------------------
# Root
# -----------------------------
@app.get("/")
def root():
    return {"status": "API is running"}


# -----------------------------
# Auth (FIXED: only one)
# -----------------------------
def check_api_key(x_api_key: str):
    if x_api_key != APP_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


# -----------------------------
# Google Sheets (FIXED)
# -----------------------------
def get_service():
    info = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


# 🔥 FIX: alias function (so your code doesn't break)
def get_sheets_service():
    return get_service()


def get_sheet_values(sheet_name: str):
    return get_service().spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{sheet_name}!A:ZZ"
    ).execute().get("values", [])


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


class UpdateLeadRequest(BaseModel):
    lead_id: str
    updates: Dict[str, Any]


# -----------------------------
# Helpers
# -----------------------------
def normalize_header(text: str) -> str:
    if not text:
        return ""
    text = str(text).replace("\n", " ").strip().lower()
    return "".join(re.findall(r"[a-z0-9]+", text))


def col_to_letter(col_num: int) -> str:
    result = ""
    while col_num > 0:
        col_num, remainder = divmod(col_num - 1, 26)
        result = chr(65 + remainder) + result
    return result


def get_headers(sheet_name: str):
    values = get_sheet_values(sheet_name)
    if len(values) < HEADER_ROW:
        return []
    return values[HEADER_ROW - 1]


def get_header_index_map(headers: List[str]):
    return {normalize_header(h): idx for idx, h in enumerate(headers)}


def rows_from_sheet(sheet_name: str):
    values = get_sheet_values(sheet_name)
    if len(values) < HEADER_ROW:
        return []

    headers = values[HEADER_ROW - 1]
    data_rows = values[DATA_START_ROW - 1:]

    rows = []
    for i, r in enumerate(data_rows):
        row = {headers[j]: r[j] if j < len(r) else "" for j in range(len(headers))}
        row["_row_number"] = DATA_START_ROW + i
        rows.append(row)

    return rows


def next_lead_id(existing_rows):
    max_num = 0
    for row in existing_rows:
        lead_id = row.get("Lead ID", "")
        match = re.match(r"L(\d+)", str(lead_id))
        if match:
            max_num = max(max_num, int(match.group(1)))
    return f"L{max_num + 1:03d}"


# -----------------------------
# APPEND (WORKING)
# -----------------------------
@app.post("/append-leads")
def append_leads(payload: AppendLeadsRequest, x_api_key: str = Header(...)):
    check_api_key(x_api_key)

    service = get_service()
    headers = get_headers(MASTER_SHEET_NAME)

    if not headers:
        raise HTTPException(status_code=404, detail="Header not found")

    header_map = get_header_index_map(headers)
    existing_rows = rows_from_sheet(MASTER_SHEET_NAME)

    inserted = []

    for lead in payload.leads:
        row_dict = {h: "" for h in headers}

        if "leadid" in header_map:
            row_dict[headers[header_map["leadid"]]] = next_lead_id(existing_rows)

        mapping = {
            "leadname": lead.lead_name,
            "company": lead.company or "",
            "source": lead.source or "",
            "stage": lead.stage_status or "",
            "lasttouchpoint": lead.last_touchpoint_date or "",
            "followupdate": lead.follow_up_date or "",
            "notes": lead.notes or "",
        }

        for key, val in mapping.items():
            if key in header_map:
                row_dict[headers[header_map[key]]] = val

        next_row = DATA_START_ROW + len(existing_rows)
        ordered = [row_dict[h] for h in headers]

        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{MASTER_SHEET_NAME}!A{next_row}",
            valueInputOption="USER_ENTERED",
            body={"values": [ordered]},
        ).execute()

        inserted.append(row_dict)
        existing_rows.append(row_dict)

    return {"status": "success", "rows_added": len(inserted)}


# -----------------------------
# UPDATE (WORKING)
# -----------------------------
@app.post("/update-lead")
def update_lead(payload: UpdateLeadRequest, x_api_key: str = Header(...)):
    check_api_key(x_api_key)

    service = get_service()
    headers = get_headers(MASTER_SHEET_NAME)
    header_map = get_header_index_map(headers)

    rows = rows_from_sheet(MASTER_SHEET_NAME)

    target = None
    for r in rows:
        if str(r.get("Lead ID", "")).strip() == payload.lead_id:
            target = r
            break

    if not target:
        raise HTTPException(status_code=404, detail="Lead not found")

    updates = []

    for key, value in payload.updates.items():
        norm = normalize_header(key)
        if norm not in header_map:
            continue

        col = header_map[norm] + 1
        col_letter = col_to_letter(col)

        updates.append({
            "range": f"{MASTER_SHEET_NAME}!{col_letter}{target['_row_number']}",
            "values": [[value]]
        })

    if not updates:
        return {"status": "no changes"}

    service.spreadsheets().values().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"valueInputOption": "USER_ENTERED", "data": updates},
    ).execute()

    return {"status": "updated"}
