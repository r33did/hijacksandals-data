import os
import pickle
import re
import webbrowser
from datetime import datetime
from pathlib import Path

import gspread
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
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

main_id = os.getenv("GDRIVE_ID")
template_id = os.getenv("TEMPLATE_ID")
scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def create_new_creds():
    creds = None

    if TOKEN_PATH.exists():
        with open(TOKEN_PATH, "rb") as f:
            creds = pickle.load(f)

    # Refresh if possible
    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
        except Exception as e:
            print(f"Refresh failed: {e}")
            creds = None

    # If no valid creds, start OAuth flow
    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file(
            str(CLIENT_SECRET),
            scope
        )

        creds = flow.run_console()  # 🔥 better for Docker

    # Save token
    TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(TOKEN_PATH, "wb") as f:
        pickle.dump(creds, f)

    return creds


def load_oauth_creds():
    if not TOKEN_PATH.exists():
        return create_new_creds()

    try:
        with open(TOKEN_PATH, "rb") as f:
            creds = pickle.load(f)

        # validate before returning
        if not creds or not creds.valid:
            return create_new_creds()

        return creds

    except Exception as e:
        print(f"Token load failed: {e}")
        return create_new_creds()

#--------
# Testing Dengan Cara lain 
#--------

# def create_new_creds():
#     creds = None

#     if TOKEN_PATH.exists():
#         with open(TOKEN_PATH, "rb") as token_file:
#             creds = pickle.load(token_file)

#     if not creds or not creds.valid:
#         try:
#             creds.refresh(Request())
#         except Exception:
#             flow = InstalledAppFlow.from_client_secrets_file(str(CLIENT_SECRET), scope)
#             creds = flow.run_local_server(port=0, open_browser=False)

#         TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
#         with open(TOKEN_PATH, "wb") as token_file:
#             pickle.dump(creds, token_file)

#     return creds


# def load_oauth_creds():
#     try : 
#         if not TOKEN_PATH.exists():
#             return create_new_creds()
#     except Exception:
#         with open(TOKEN_PATH, "rb") as token_file:
#                 return pickle.load(token_file)
#     else : 
#         return create_new_creds()


class sheetdrive:
    def __init__(self):
        self.creds = load_oauth_creds()
        self.main_id = main_id
        self.template_id = template_id

    def set_main_id(self, folder_id: str):
        if folder_id:
            self.main_id = folder_id.strip()
        return self.main_id

    def service(self):
        return build("drive", "v3", credentials=self.creds)

    def connect_gspread(self):
        return gspread.authorize(self.creds)

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

    def copy_template(self, file_name: str, new_title: str):
        drive_service = self.service()
        file_metadata = {"name": new_title, "parents": [self.template_id]}
        file_id = self.search_filename(file_name=file_name, folder_id=self.template_id)

        copied_file = drive_service.files().copy(
            fileId=file_id,
            body=file_metadata,
            supportsAllDrives=True,
        ).execute()

        return copied_file.get("id")

    def upload_new_gsheet(self, dataframe, spreadsheet_name=None, open_browser=False):
        if not spreadsheet_name:
            spreadsheet_name = f"Extract_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        client = gspread.authorize(self.creds)
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

        client = gspread.authorize(self.creds)
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
