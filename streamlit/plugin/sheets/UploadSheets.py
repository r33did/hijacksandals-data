import base64
import os
import pickle
import re
import webbrowser
from datetime import datetime
import json
from pathlib import Path
from urllib.parse import urlencode
from urllib.parse import urlparse

import gspread
import pandas as pd
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import Flow, InstalledAppFlow
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
USER_TOKEN_DIR = STREAMLIT_DIR / "creds" / "user_tokens"
USER_OAUTH_STATE_DIR = STREAMLIT_DIR / "creds" / "user_oauth_state"

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
        
        try : 
            creds = flow.run_console() # 🔥 better for Docker
        except : 
            creds = flow.run_local_server(port=0, open_browser=False)  

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
    
def call_service():
    creds = Credentials.from_service_account_file(
    str(SERVICE_ACCOUNT),
    scopes=scope)

    return creds


def load_service_account_email() -> str:
    with open(SERVICE_ACCOUNT, "r", encoding="utf-8") as service_account_file:
        payload = json.load(service_account_file)

    service_account_email = str(payload.get("client_email", "")).strip()
    if not service_account_email:
        raise ValueError("Service account email is not configured in the credentials file.")

    return service_account_email


def sanitize_token_key(raw_value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9_.-]+", "_", str(raw_value or "").strip())
    return normalized.strip("._-") or "default"


def get_user_token_path(token_key: str) -> Path:
    USER_TOKEN_DIR.mkdir(parents=True, exist_ok=True)
    return USER_TOKEN_DIR / f"{sanitize_token_key(token_key)}.pickle"


def get_user_oauth_state_path(token_key: str) -> Path:
    USER_OAUTH_STATE_DIR.mkdir(parents=True, exist_ok=True)
    return USER_OAUTH_STATE_DIR / f"{sanitize_token_key(token_key)}.pickle"


def load_oauth_client_config(redirect_uri: str) -> dict:
    with open(CLIENT_SECRET, "r", encoding="utf-8") as client_secret_file:
        raw_payload = json.load(client_secret_file)

    client_type = "web" if "web" in raw_payload else "installed"
    client_payload = raw_payload.get(client_type, raw_payload)
    return {
        client_type: {
            "client_id": client_payload["client_id"],
            "client_secret": client_payload["client_secret"],
            "auth_uri": client_payload.get("auth_uri", "https://accounts.google.com/o/oauth2/auth"),
            "token_uri": client_payload.get("token_uri", "https://oauth2.googleapis.com/token"),
            "auth_provider_x509_cert_url": client_payload.get(
                "auth_provider_x509_cert_url",
                "https://www.googleapis.com/oauth2/v1/certs",
            ),
            "redirect_uris": [redirect_uri],
        }
    }


def build_oauth_state(app_username: str, page_name: str, action_name: str) -> str:
    payload = {
        "username": str(app_username or "").strip(),
        "page": str(page_name or "").strip(),
        "action": str(action_name or "").strip(),
    }
    encoded = base64.urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode("utf-8")
    return encoded.rstrip("=")


def parse_oauth_state(state_value: str | None) -> dict:
    if not state_value:
        return {}

    padded = state_value + "=" * (-len(state_value) % 4)
    try:
        decoded = base64.urlsafe_b64decode(padded.encode("utf-8")).decode("utf-8")
        return json.loads(decoded)
    except Exception:
        return {}


def configure_oauth_transport_for_redirect_uri(redirect_uri: str):
    parsed_uri = urlparse(str(redirect_uri or "").strip())
    is_local_http = (
        parsed_uri.scheme == "http"
        and parsed_uri.hostname in {"localhost", "127.0.0.1", "::1"}
    )
    if is_local_http:
        os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"


def build_user_oauth_authorization_url(redirect_uri: str, state: str) -> str:
    configure_oauth_transport_for_redirect_uri(redirect_uri)
    flow = Flow.from_client_config(
        load_oauth_client_config(redirect_uri),
        scopes=scope,
        state=state,
        autogenerate_code_verifier=True,
    )
    flow.redirect_uri = redirect_uri
    authorization_url, _ = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
    )
    return authorization_url, getattr(flow, "code_verifier", None)


def build_authorization_response_url(redirect_uri: str, params: dict) -> str:
    filtered_params = {key: value for key, value in params.items() if value is not None}
    return f"{redirect_uri}?{urlencode(filtered_params, doseq=True)}"


def exchange_user_oauth_code(
    redirect_uri: str,
    state: str,
    authorization_response: str,
    code_verifier: str | None = None,
):
    configure_oauth_transport_for_redirect_uri(redirect_uri)
    flow = Flow.from_client_config(load_oauth_client_config(redirect_uri), scopes=scope, state=state)
    flow.redirect_uri = redirect_uri
    if code_verifier:
        flow.code_verifier = code_verifier
    flow.fetch_token(authorization_response=authorization_response)
    return flow.credentials


def save_user_oauth_creds(token_key: str, creds) -> Path:
    token_path = get_user_token_path(token_key)
    with open(token_path, "wb") as token_file:
        pickle.dump(creds, token_file)
    return token_path


def load_user_oauth_creds(token_key: str):
    token_path = get_user_token_path(token_key)
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
            save_user_oauth_creds(token_key, creds)
        except Exception:
            return None

    if not creds or not creds.valid:
        return None

    return creds


def delete_user_oauth_creds(token_key: str):
    token_path = get_user_token_path(token_key)
    if token_path.exists():
        token_path.unlink()


def save_user_oauth_state(token_key: str, payload: dict) -> Path:
    state_path = get_user_oauth_state_path(token_key)
    with open(state_path, "wb") as state_file:
        pickle.dump(payload, state_file)
    return state_path


def load_user_oauth_state(token_key: str) -> dict | None:
    state_path = get_user_oauth_state_path(token_key)
    if not state_path.exists():
        return None

    try:
        with open(state_path, "rb") as state_file:
            payload = pickle.load(state_file)
    except Exception:
        return None

    return payload if isinstance(payload, dict) else None


def delete_user_oauth_state(token_key: str):
    state_path = get_user_oauth_state_path(token_key)
    if state_path.exists():
        state_path.unlink()

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
    def __init__(self, creds=None):
        self.creds = creds or call_service()
        self.service_account_email = load_service_account_email()
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

    def _resolve_parent_folder_id(self, destination_folder_id: str | None = None) -> str:
        return (destination_folder_id or main_id or self.main_id or template_id or self.template_id or "").strip()

    def ensure_service_account_access(self, file_id: str, role: str = "writer"):
        if not file_id or not self.service_account_email:
            return

        drive_service = self.service()
        permissions = drive_service.permissions().list(
            fileId=file_id,
            fields="permissions(id,emailAddress,role,type)",
            supportsAllDrives=True,
        ).execute().get("permissions", [])

        existing_permission_id = None
        existing_role = None
        for permission in permissions:
            if (
                permission.get("type") == "user"
                and str(permission.get("emailAddress", "")).lower() == self.service_account_email.lower()
            ):
                existing_permission_id = permission.get("id")
                existing_role = permission.get("role")
                break

        stronger_roles = {"owner", "organizer", "fileOrganizer", "writer"}
        if existing_role in stronger_roles:
            return

        if existing_permission_id:
            drive_service.permissions().update(
                fileId=file_id,
                permissionId=existing_permission_id,
                body={"role": role},
                supportsAllDrives=True,
            ).execute()
            return

        drive_service.permissions().create(
            fileId=file_id,
            body={
                "type": "user",
                "role": role,
                "emailAddress": self.service_account_email,
            },
            sendNotificationEmail=False,
            supportsAllDrives=True,
        ).execute()

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

    def copy_template(
        self,
        file_name: str | None = None,
        new_title: str | None = None,
        destination_folder_id: str | None = None,
        template_file_id: str | None = None,
    ):
        drive_service = self.service()
        target_folder_id = self._resolve_parent_folder_id(destination_folder_id)
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

        copied_file_id = copied_file.get("id")
        self.ensure_service_account_access(copied_file_id)
        return copied_file_id

    def create_from_blueprint(
        self,
        blueprint_path: str | Path,
        spreadsheet_name: str | None = None,
        destination_folder_id: str | None = None,
    ) -> str:
        blueprint_file = Path(blueprint_path)
        if not blueprint_file.exists():
            raise FileNotFoundError(f"Blueprint file not found: {blueprint_file}")

        with open(blueprint_file, "r", encoding="utf-8") as blueprint_handle:
            blueprint = json.load(blueprint_handle)

        worksheets = blueprint.get("worksheets", [])
        if not worksheets:
            raise ValueError("Blueprint does not define any worksheets.")

        sheet_order = blueprint.get("workbook", {}).get("sheet_order") or [item.get("name") for item in worksheets]
        ordered_names = [str(name).strip() for name in sheet_order if str(name).strip()]
        if not ordered_names:
            raise ValueError("Blueprint does not define a valid sheet order.")

        target_folder_id = self._resolve_parent_folder_id(destination_folder_id)
        if not target_folder_id:
            raise ValueError("Drive destination folder ID is not configured.")

        drive_service = self.service()
        client = self.connect_gspread()
        file_metadata = {
            "name": (spreadsheet_name or blueprint_file.stem).strip(),
            "mimeType": "application/vnd.google-apps.spreadsheet",
            "parents": [target_folder_id],
        }
        created_file = drive_service.files().create(body=file_metadata, fields="id").execute()
        spreadsheet_id = created_file.get("id")
        self.ensure_service_account_access(spreadsheet_id)
        spreadsheet = client.open_by_key(spreadsheet_id)

        first_sheet = spreadsheet.sheet1
        first_sheet_name = ordered_names[0]
        first_sheet.update_title(first_sheet_name)

        existing_sheet_names = {worksheet.title for worksheet in spreadsheet.worksheets()}
        for sheet_name in ordered_names[1:]:
            if sheet_name not in existing_sheet_names:
                spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="26")
                existing_sheet_names.add(sheet_name)

        worksheet_map = {worksheet.title: worksheet for worksheet in spreadsheet.worksheets()}
        for worksheet_spec in worksheets:
            sheet_name = str(worksheet_spec.get("name") or "").strip()
            if not sheet_name or sheet_name not in worksheet_map:
                continue

            worksheet = worksheet_map[sheet_name]
            self._apply_blueprint_sheet(worksheet, worksheet_spec)

        return spreadsheet_id

    def _apply_blueprint_sheet(self, worksheet, worksheet_spec: dict):
        headers = worksheet_spec.get("headers", {})
        if "values_a_to_r" in headers:
            header_values = [value if value is not None else "" for value in headers["values_a_to_r"]]
            worksheet.update("A1:R1", [header_values], raw=False)
        elif "values_a_to_c" in headers:
            header_values = [value if value is not None else "" for value in headers["values_a_to_c"]]
            worksheet.update("A1:C1", [header_values], raw=False)

        seed_rows = worksheet_spec.get("seed_rows") or worksheet_spec.get("sample_rows") or []
        if seed_rows:
            normalized_rows = self._normalize_blueprint_rows(seed_rows)
            width = max(len(row) for row in normalized_rows)
            end_column = gspread.utils.rowcol_to_a1(1, width).rstrip("1")
            worksheet.update(f"A2:{end_column}{len(normalized_rows) + 1}", normalized_rows, raw=False)

        formulas = worksheet_spec.get("formulas", [])
        for formula_spec in formulas:
            cell = formula_spec.get("cell")
            formula = formula_spec.get("formula")
            if not cell or not formula:
                continue
            worksheet.update(cell, [[formula]], raw=False)

        role = worksheet_spec.get("role")
        if role == "summary_report":
            notes = worksheet_spec.get("rebuild_notes") or []
            if notes:
                worksheet.update("A1", [[notes[0]]], raw=False)

    def _normalize_blueprint_rows(self, rows: list[list]):
        normalized_rows = []
        for row in rows:
            normalized_row = []
            for value in row:
                if isinstance(value, dict) and "formula" in value:
                    normalized_row.append(value["formula"])
                elif value is None:
                    normalized_row.append("")
                else:
                    normalized_row.append(value)
            normalized_rows.append(normalized_row)
        return normalized_rows

    def materialize_blueprint_reports(
        self,
        spreadsheet_id: str,
        blueprint_path: str | Path,
    ):
        blueprint_file = Path(blueprint_path)
        with open(blueprint_file, "r", encoding="utf-8") as blueprint_handle:
            blueprint = json.load(blueprint_handle)

        worksheets = blueprint.get("worksheets", [])
        if not worksheets:
            return

        client = self.connect_gspread()
        spreadsheet = client.open_by_key(spreadsheet_id)
        worksheet_map = {worksheet.title: worksheet for worksheet in spreadsheet.worksheets()}
        worksheet_specs = {
            str(item.get("name") or "").strip(): item
            for item in worksheets
            if str(item.get("name") or "").strip()
        }

        for worksheet_name, worksheet_spec in worksheet_specs.items():
            if worksheet_name in worksheet_map and worksheet_spec.get("role") == "lookup_mapping":
                self._apply_blueprint_sheet(worksheet_map[worksheet_name], worksheet_spec)

        for worksheet_name, worksheet_spec in worksheet_specs.items():
            pivot_spec = worksheet_spec.get("pivot_spec")
            if not pivot_spec or worksheet_name not in worksheet_map:
                continue
            source_sheet_name = str(pivot_spec.get("source_sheet") or "").strip()
            if not source_sheet_name or source_sheet_name not in worksheet_map:
                continue

            source_records = worksheet_map[source_sheet_name].get_all_records()
            source_df = pd.DataFrame(source_records)
            if source_df.empty:
                continue

            summary_df = self._build_summary_from_pivot_spec(source_df, pivot_spec)
            target_sheet = worksheet_map[worksheet_name]
            target_sheet.clear()
            if summary_df.empty:
                target_sheet.update("A1", [["No data available for this summary."]], raw=False)
                continue
            set_with_dataframe(target_sheet, summary_df, include_index=False, include_column_header=True)

    def _build_summary_from_pivot_spec(self, source_df: pd.DataFrame, pivot_spec: dict) -> pd.DataFrame:
        row_groups = [field for field in pivot_spec.get("row_groups", []) if field in source_df.columns]
        column_groups = [field for field in pivot_spec.get("column_groups", []) if field in source_df.columns]
        value_specs = [item for item in pivot_spec.get("values", []) if item.get("source_field") in source_df.columns]

        if not value_specs:
            return pd.DataFrame()

        working_df = source_df.copy()
        for spec in value_specs:
            field_name = spec["source_field"]
            working_df[field_name] = pd.to_numeric(working_df[field_name], errors="coerce").fillna(0)

        value_fields = [spec["source_field"] for spec in value_specs]
        aggfunc = "sum"
        summary = pd.pivot_table(
            working_df,
            index=row_groups or None,
            columns=column_groups or None,
            values=value_fields,
            aggfunc=aggfunc,
            fill_value=0,
        )

        if isinstance(summary, pd.Series):
            summary = summary.to_frame()

        if isinstance(summary.columns, pd.MultiIndex):
            summary.columns = [
                " | ".join(str(part) for part in column_parts if part not in (None, ""))
                for column_parts in summary.columns.to_flat_index()
            ]
        else:
            summary.columns = [str(column) for column in summary.columns]

        if any(spec.get("display_name") for spec in value_specs) and not column_groups and len(value_specs) == len(summary.columns):
            rename_map = {
                spec["source_field"]: spec.get("display_name") or spec["source_field"]
                for spec in value_specs
            }
            summary = summary.rename(columns=rename_map)

        return summary.reset_index()

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
        self.ensure_service_account_access(sheet_id)
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

        self.ensure_service_account_access(file_id)

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
