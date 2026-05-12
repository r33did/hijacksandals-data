import base64
import json
import os
import pickle
import re
import webbrowser
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import gspread
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import Flow
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from gspread_dataframe import set_with_dataframe

BASE_DIR = Path(__file__).resolve()
ROOT_DIR = BASE_DIR.parents[3]
STREAMLIT_DIR = ROOT_DIR / "streamlit"
ENV_PATH = ROOT_DIR / ".env"

load_dotenv(ENV_PATH)


def resolve_env_path(env_key: str) -> Path:
    raw_value = os.getenv(env_key, "").strip()
    if not raw_value:
        raise ValueError(f"Environment variable `{env_key}` is not configured.")

    normalized = Path(raw_value.replace("\\", "/"))
    candidate_paths = []

    if normalized.is_absolute():
        candidate_paths.append(normalized)

        parts = normalized.parts
        if len(parts) >= 3 and parts[1] == "app" and parts[2] == "streamlit":
            candidate_paths.append(STREAMLIT_DIR.joinpath(*parts[3:]))
        elif len(parts) >= 2 and parts[1] == "app":
            candidate_paths.append(ROOT_DIR.joinpath(*parts[2:]))

    relative_parts = normalized.parts
    if relative_parts and relative_parts[0] == "streamlit":
        relative_parts = relative_parts[1:]

    candidate_paths.append(ROOT_DIR / normalized)
    candidate_paths.append(STREAMLIT_DIR / Path(*relative_parts) if relative_parts else STREAMLIT_DIR)

    for candidate in candidate_paths:
        resolved = Path(candidate)
        if resolved.exists():
            return resolved

    return Path(candidate_paths[-1])


TOKEN_PATH = resolve_env_path("PICKLE_CRED")
CLIENT_SECRET = resolve_env_path("OAUTH")
SERVICE_ACCOUNT = resolve_env_path("CREDS")

main_id = os.getenv("GDRIVE_ID")
template_id = os.getenv("TEMPLATE_ID")
scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _sanitize_token_key(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9_.-]+", "_", (value or "").strip())
    return sanitized.strip("._") or "default"


def get_user_token_path(username: str) -> Path:
    token_filename = f"{TOKEN_PATH.stem}_{_sanitize_token_key(username)}{TOKEN_PATH.suffix or '.pickle'}"
    return TOKEN_PATH.with_name(token_filename)


def normalize_redirect_uri(raw_url: str) -> str:
    normalized = (raw_url or "").strip()
    if not normalized:
        raise ValueError("Google OAuth redirect URI is empty.")

    parsed = urlsplit(normalized)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError("Google OAuth redirect URI must include scheme and host.")

    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path or "/", "", ""))


def build_oauth_state(username: str, page: str = "Home", action: str = "connect google drive") -> str:
    payload = {
        "username": username,
        "page": page,
        "action": action,
    }
    encoded = base64.urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")
    return encoded.rstrip("=")


def decode_oauth_state(state: str) -> dict:
    if not state:
        return {}

    padded_state = state + "=" * (-len(state) % 4)
    try:
        decoded = base64.urlsafe_b64decode(padded_state.encode("ascii")).decode("utf-8")
        return json.loads(decoded)
    except Exception:
        return {}


def build_oauth_flow(redirect_uri: str, state: str | None = None) -> Flow:
    flow = Flow.from_client_secrets_file(str(CLIENT_SECRET), scopes=scope, state=state)
    flow.redirect_uri = normalize_redirect_uri(redirect_uri)
    return flow


def begin_oauth_flow(username: str, redirect_uri: str, page: str = "Home", action: str = "connect google drive"):
    state = build_oauth_state(username=username, page=page, action=action)
    flow = build_oauth_flow(redirect_uri=redirect_uri, state=state)
    authorization_url, _ = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
    )
    return authorization_url, state


def save_oauth_creds(username: str, creds):
    token_path = get_user_token_path(username)
    token_path.parent.mkdir(parents=True, exist_ok=True)
    with open(token_path, "wb") as token_file:
        pickle.dump(creds, token_file)


def load_oauth_creds(username: str):
    token_path = get_user_token_path(username)
    if not token_path.exists():
        return None

    try:
        with open(token_path, "rb") as token_file:
            creds = pickle.load(token_file)
    except Exception:
        return None

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            save_oauth_creds(username, creds)
        except Exception:
            return None

    if not creds or not creds.valid:
        return None

    return creds


def complete_oauth_flow(username: str, redirect_uri: str, code: str, state: str):
    flow = build_oauth_flow(redirect_uri=redirect_uri, state=state)
    flow.fetch_token(code=code)
    creds = flow.credentials
    save_oauth_creds(username, creds)
    return creds


def clear_oauth_creds(username: str):
    token_path = get_user_token_path(username)
    if token_path.exists():
        token_path.unlink()


def has_oauth_creds(username: str) -> bool:
    return load_oauth_creds(username) is not None


def call_service():
    return Credentials.from_service_account_file(str(SERVICE_ACCOUNT), scopes=scope)


class sheetdrive:
    def __init__(self, auth_mode: str = "service_account", oauth_user: str | None = None):
        self.auth_mode = auth_mode
        self.oauth_user = oauth_user
        self._service_account_creds = None
        self.main_id = main_id
        self.template_id = template_id

    def set_auth_mode(self, auth_mode: str):
        self.auth_mode = auth_mode
        return self.auth_mode

    def set_oauth_user(self, username: str | None):
        self.oauth_user = username
        return self.oauth_user

    def set_main_id(self, folder_id: str):
        if folder_id:
            self.main_id = folder_id.strip()
        return self.main_id

    def is_ready(self) -> bool:
        try:
            self.get_creds()
            return True
        except Exception:
            return False

    def get_creds(self):
        if self.auth_mode == "oauth":
            if not self.oauth_user:
                raise RuntimeError("No app user is attached to the Google Drive OAuth session.")

            creds = load_oauth_creds(self.oauth_user)
            if not creds:
                raise RuntimeError("Google Drive is not connected for this app user.")
            return creds

        if self._service_account_creds is None:
            self._service_account_creds = call_service()
        return self._service_account_creds

    def service(self):
        return build("drive", "v3", credentials=self.get_creds())

    def connect_gspread(self):
        return gspread.authorize(self.get_creds())

    def search_filename(self, file_name: str, folder_id: str):
        driver_service = self.service()
        list_file = driver_service.files().list(
            q=f"'{folder_id}' in parents",
            fields="files(name,id,mimeType)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        regex_filename = f".*{file_name}.*"

        for file_item in list_file["files"]:
            if re.search(regex_filename, file_item["name"]):
                return file_item["id"]

        return None

    def copy_template(
        self,
        file_name: str | None = None,
        new_title: str | None = None,
        destination_folder_id: str | None = None,
        template_file_id: str | None = None,
    ):
        drive_service = self.service()
        target_folder_id = (destination_folder_id or self.main_id or self.template_id or "").strip()
        if not target_folder_id:
            raise ValueError("Drive destination folder ID is not configured.")

        file_id = (template_file_id or "").strip()
        if not file_id:
            if not file_name:
                raise ValueError("Template file name or template file ID is required.")
            file_id = self.search_filename(file_name=file_name, folder_id=self.template_id)
        if not file_id:
            raise ValueError("Template spreadsheet could not be found in the template Drive folder.")

        file_metadata = {
            "name": (new_title or file_name or f"Copied_{datetime.now().strftime('%Y%m%d_%H%M%S')}").strip(),
            "parents": [target_folder_id],
        }

        copied_file = drive_service.files().copy(
            fileId=file_id,
            body=file_metadata,
            supportsAllDrives=True,
        ).execute()

        return copied_file.get("id")

    def upload_new_gsheet(self, dataframe, spreadsheet_name=None, open_browser=False):
        if not spreadsheet_name:
            spreadsheet_name = f"Extract_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        client = self.connect_gspread()
        drive_service = self.service()

        file_metadata = {
            "name": spreadsheet_name,
            "mimeType": "application/vnd.google-apps.spreadsheet",
            "parents": [self.main_id],
        }

        file = drive_service.files().create(body=file_metadata, fields="id").execute()
        sheet_id = file.get("id")
        spreadsheet = client.open_by_key(sheet_id)

        spreadsheet.share(None, perm_type="anyone", role="writer")

        worksheet = spreadsheet.sheet1
        set_with_dataframe(worksheet, dataframe)

        if open_browser:
            webbrowser.open(spreadsheet.url)

        return spreadsheet.url

    def update_gsheet(
        self,
        file_name: str = None,
        dataframe=None,
        sheet_name="Main_Data",
        overwrite=False,
        spreadsheet_id: str = None,
        open_browser=False,
    ):
        file_id = spreadsheet_id
        if not file_id:
            file_id = self.search_filename(file_name=file_name, folder_id=self.main_id)
        if not file_id:
            raise ValueError("Spreadsheet target could not be found.")

        client = self.connect_gspread()
        spreadsheet = client.open_by_key(file_id)
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(
                title=sheet_name,
                rows="1000",
                cols="20",
            )

        if overwrite:
            worksheet.clear()

        set_with_dataframe(worksheet, dataframe)

        if open_browser:
            webbrowser.open(spreadsheet.url)

        return spreadsheet.url

    def list_folder(self, folder_id=None, include=".*report.*"):
        if folder_id is None:
            folder_id = self.main_id
        drive_service = self.service()
        res = drive_service.files().list(
            q=f"'{folder_id}' in parents",
            fields="files(name,id,mimeType)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        list_file = res.get("files", [])
        list_final_file = []
        for file_item in list_file:
            if re.search(include, file_item["name"].lower()) and re.search("spreadsheet", file_item["mimeType"]):
                list_final_file.append((file_item["name"], file_item["id"]))
        return list_final_file

    def get_or_create_sheet(self, spreadsheet_id: str, sheet_name: str):
        client = self.connect_gspread()
        spreadsheet = client.open_by_key(spreadsheet_id)

        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(
                title=sheet_name,
                rows="1000",
                cols="20",
            )

        return worksheet
