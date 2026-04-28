from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, date, timedelta
import os, json, re

from google.oauth2 import service_account
from googleapiclient.discovery import build

app = FastAPI(title="Sales Handoff API", version="6.0.0")

# ---------------- ENV ----------------
APP_API_KEY = os.getenv("APP_API_KEY")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
MASTER_SHEET_NAME = os.getenv("MASTER_SHEET_NAME", "Master Leads")
LOG_SHEET_NAME = os.getenv("LOG_SHEET_NAME", "Update Log")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
HEADER_ROW = 3

# ---------------- AUTH ----------------
def check_api_key(x_api_key: str):
    if x_api_key != APP_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

# ---------------- GOOGLE ----------------
def get_service():
    info = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)

def get_values(sheet):
    return get_service().spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{sheet}!A:ZZ"
    ).execute().get("values", [])

# ---------------- MODELS ----------------
class LeadRecord(BaseModel):
    lead_name: str
    company: Optional[str] = ""
    source: Optional[str] = ""
    stage_status: Optional[str] = ""
    last_touchpoint_date: Optional[str] = ""
    follow_up_date: Optional[str] = ""
    notes: Optional[str] = ""

class AppendReq(BaseModel):
    leads: List[LeadRecord]

class ReviewReq(BaseModel):
    period: str = "month"

# ---------------- HELPERS ----------------
def norm(x):
    return re.sub(r'[^a-z0-9]', '', str(x).lower())

def headers():
    vals = get_values(MASTER_SHEET_NAME)
    return vals[HEADER_ROW-1] if len(vals) >= HEADER_ROW else []

def header_map(h):
    return {norm(c): i for i, c in enumerate(h)}

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
    if not s: return ""
    s = s.lower()
    if "ref" in s: return "Referral"
    if "link" in s: return "LinkedIn"
    if "webinar" in s: return "Webinar"
    if "cold" in s: return "Cold Email"
    return s.title()

def parse_date(x):
    try:
        return datetime.strptime(x, "%Y-%m-%d").date()
    except:
        return None

def period_range(p):
    today = date.today()
    if p == "today":
        return today, today
    if p == "week":
        start = today - timedelta(days=today.weekday())
        return start, start + timedelta(days=6)
    if p == "month":
        start = today.replace(day=1)
        end = (start + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        return start, end

# ---------------- LOGGING ----------------
def log_entry(msg):
    try:
        get_service().spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{LOG_SHEET_NAME}!A1",
            valueInputOption="USER_ENTERED",
            body={"values":[[datetime.utcnow(), msg]]}
        ).execute()
    except:
        pass

# ---------------- APPEND ----------------
@app.post("/append-leads")
def append(payload: AppendReq, x_api_key: str = Header(...)):
    check_api_key(x_api_key)

    h = headers()
    if not h:
        raise HTTPException(404, "Header missing")

    m = header_map(h)
    inserted = []

    for lead in payload.leads:
        row = [""] * len(h)

        def set_val(k,v):
            if norm(k) in m:
                row[m[norm(k)]] = v

        lead_id = next_lead_id()

        set_val("lead id", lead_id)
        set_val("lead name", lead.lead_name)
        set_val("company", lead.company)
        set_val("source", normalize_source(lead.source))
        set_val("stage", lead.stage_status)
        set_val("last touchpoint", lead.last_touchpoint_date)
        set_val("follow up date", lead.follow_up_date)
        set_val("notes", lead.notes)

        r = next_row()

        get_service().spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{MASTER_SHEET_NAME}!A{r}",
            valueInputOption="USER_ENTERED",
            body={"values":[row]}
        ).execute()

        inserted.append({"row": r, "lead_id": lead_id})

        log_entry(f"Added {lead.lead_name}")

    return {"status":"success","inserted":inserted}

# ---------------- REVIEW ----------------
@app.post("/get-review-data")
def review(payload: ReviewReq, x_api_key: str = Header(...)):
    check_api_key(x_api_key)

    rows = get_values(MASTER_SHEET_NAME)
    data = rows[HEADER_ROW:]

    start, end = period_range(payload.period)

    ready, blocked, stale = [], [], []

    for r in data:
        if len(r) < 6:
            continue

        name = r[1]
        source = r[3]
        stage = r[4]
        touch = parse_date(r[5])

        if not name or not source or not stage:
            blocked.append(name)
        elif touch and (date.today() - touch).days > 30:
            stale.append(name)
        else:
            ready.append(name)

    return {
        "ready": ready,
        "blocked": blocked,
        "stale": stale
    }

# ---------------- MESSAGE DRAFT ----------------
@app.post("/draft-message")
def draft(x_api_key: str = Header(...)):
    check_api_key(x_api_key)

    rows = get_values(MASTER_SHEET_NAME)
    data = rows[HEADER_ROW:]

    leads = [r[1] for r in data if len(r) > 1]

    whatsapp = "Today's Leads:\n" + "\n".join(leads[:5])

    email = f"""
Subject: Sales Handoff Summary

Hi Team,

Leads ready:
{', '.join(leads[:5])}

Please take action.

Thanks
"""

    return {
        "whatsapp": whatsapp,
        "email": email
    }
