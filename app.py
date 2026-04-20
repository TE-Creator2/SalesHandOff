from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import os
import json

from google.oauth2 import service_account
from googleapiclient.discovery import build

app = FastAPI(title="Sales Handoff API", version="1.0.0")

APP_API_KEY = os.getenv("APP_API_KEY")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
MASTER_SHEET_NAME = os.getenv("MASTER_SHEET_NAME", "Master Leads")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def get_sheets_service():
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw:
        raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_JSON")
    info = json.loads(raw)
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)

def check_api_key(x_api_key: str):
    if x_api_key != APP_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

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

@app.get("/sheet-schema")
def sheet_schema(x_api_key: str = Header(...)):
    check_api_key(x_api_key)
    return {
        "spreadsheet_id": SPREADSHEET_ID,
        "tabs": ["Master Leads", "Learning Log"],
        "master_leads_columns": [
            "Lead_ID", "Lead_Name", "Company", "Source", "Owner",
            "Stage_Status", "Last_Touchpoint_Date", "Last_Touchpoint_Summary",
            "Follow_Up_Date", "Requirement_Interest", "Notes",
            "Missing_Fields_Declared"
        ]
    }

@app.post("/append-leads")
def append_leads(payload: AppendLeadsRequest, x_api_key: str = Header(...)):
    check_api_key(x_api_key)
    service = get_sheets_service()

    values = []
    for lead in payload.leads:
        values.append([
            "",  # Lead_ID can be filled by sheet logic or backend logic
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

    body = {"values": values}
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{MASTER_SHEET_NAME}!A:L",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body,
    ).execute()

    return {"status": "ok", "rows_added": len(values)}