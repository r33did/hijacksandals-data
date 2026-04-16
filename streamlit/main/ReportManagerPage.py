import base64
import datetime
import json
import os
from pathlib import Path
from typing import Any
from urllib import error, request

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from plugin.postgre.QueryBuilder import Engine, ReadTemplate
from plugin.sheets.UploadSheets import sheetdrive as SheetDrive

engine = Engine()
sheetdrive = SheetDrive()
client = sheetdrive.connect_gspread()
templatequery = ReadTemplate()

BASE_DIR = Path(__file__).resolve()
ROOT_DIR = BASE_DIR.parents[1]
CONFIG_DIR = BASE_DIR.parent / "config"
SCHEDULE_REGISTRY_PATH = CONFIG_DIR / "scheduled_reports.json"
ENV_PATH = ROOT_DIR / ".env"

load_dotenv(ENV_PATH)


@st.cache_data(ttl=300)
def get_report_list(folder_id: str):
    return dict(sheetdrive.list_folder(folder_id=folder_id))


def time_to_cron(
    minute: int = 0,
    hour: int = 0,
    day_of_month: str = "*",
    month: str = "*",
    day_of_week: str = "*",
) -> str:
    return f"{minute} {hour} {day_of_month} {month} {day_of_week}"


def init_session_state():
    defaults = {
        "refresh_use_url": False,
        "schedule_use_url": False,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def ensure_schedule_registry():
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    if not SCHEDULE_REGISTRY_PATH.exists():
        with open(SCHEDULE_REGISTRY_PATH, "w", encoding="utf-8") as schedule_file:
            json.dump({"schedules": []}, schedule_file, indent=2)


def load_schedule_registry() -> list[dict[str, Any]]:
    ensure_schedule_registry()
    with open(SCHEDULE_REGISTRY_PATH, "r", encoding="utf-8") as schedule_file:
        payload = json.load(schedule_file)
    return payload.get("schedules", [])


def save_schedule_registry(schedules: list[dict[str, Any]]):
    ensure_schedule_registry()
    with open(SCHEDULE_REGISTRY_PATH, "w", encoding="utf-8") as schedule_file:
        json.dump({"schedules": schedules}, schedule_file, indent=2)


def upsert_schedule_registry(schedule_payload: dict[str, Any]):
    schedules = load_schedule_registry()
    remaining_schedules = [item for item in schedules if item.get("dag_id") != schedule_payload.get("dag_id")]
    remaining_schedules.append(schedule_payload)
    save_schedule_registry(sorted(remaining_schedules, key=lambda item: item.get("created_at", ""), reverse=True))


def delete_schedule_registry(dag_id: str):
    schedules = load_schedule_registry()
    save_schedule_registry([item for item in schedules if item.get("dag_id") != dag_id])


def parse_spreadsheet_id(raw_value: str | None) -> str | None:
    if not raw_value:
        return None

    cleaned_value = raw_value.strip()
    if "/d/" in cleaned_value:
        parts = cleaned_value.split("/d/", maxsplit=1)[1]
        return parts.split("/", maxsplit=1)[0]
    return cleaned_value or None


def normalize_dag_name(value: str) -> str:
    normalized = "".join(character if character.isalnum() or character == "_" else "_" for character in value.strip())
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    return normalized.strip("_")


def build_dag_id(template_name: str, sheet_id: str) -> str:
    return normalize_dag_name(f"{template_name}_{sheet_id}")


def build_spreadsheet_url(sheet_id: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}"


def build_template_catalog() -> pd.DataFrame:
    return pd.DataFrame(templatequery.get_template_context()).rename(
        columns={"template_name": "Template Name", "description": "Description"}
    )


def render_page_tutorial():
    with st.expander("How this page works", expanded=True):
        st.markdown(
            "\n".join(
                [
                    "1. `Refresh Data` updates worksheet tabs whose names already match saved templates.",
                    "2. `Generate & Set Schedule` creates or overwrites one selected sheet using the chosen template and saves schedule metadata.",
                    "3. `Delete Schedule` lists saved DAGs, shows their details, and removes the selected schedule.",
                ]
            )
        )


def render_template_context():
    st.subheader("Template Context")
    template_catalog = build_template_catalog()
    if template_catalog.empty:
        st.info("No template query is available yet.")
        return
    st.dataframe(template_catalog, hide_index=True, use_container_width=True)


def render_spreadsheet_selector(report_options: dict[str, str], state_prefix: str) -> str | None:
    input_col, toggle_col = st.columns([1, 1])
    selected_report_id = None
    option_names = sorted(report_options.keys())

    with input_col:
        if st.session_state.get(f"{state_prefix}_use_url", False):
            input_value = st.text_input(
                "Input Report Url or Spreadsheet ID",
                placeholder="https://docs.google.com/spreadsheets/d/{ID}/edit",
                key=f"{state_prefix}_spreadsheet_input",
            )
            selected_report_id = parse_spreadsheet_id(input_value)
            if input_value and not selected_report_id:
                st.warning("Spreadsheet ID could not be read from the input.")
        else:
            if option_names:
                selected_report_name = st.selectbox(
                    "Select Report",
                    options=option_names,
                    key=f"{state_prefix}_spreadsheet_select",
                )
                selected_report_id = report_options.get(selected_report_name)
            else:
                st.selectbox(
                    "Select Report",
                    options=["No spreadsheet found"],
                    disabled=True,
                    key=f"{state_prefix}_spreadsheet_select_empty",
                )

    with toggle_col:
        st.write("")
        st.write("")
        st.toggle(
            "Input by URL",
            key=f"{state_prefix}_use_url",
            help="Turn on if you want to paste the spreadsheet URL directly.",
        )

    return selected_report_id


def run_template_query(template_name: str) -> pd.DataFrame:
    return engine.execute_query(templatequery.get_query(template_name), params=None)


def preview_template_query(template_name: str) -> tuple[str, pd.DataFrame]:
    query_text = (templatequery.get_query(template_name) or "").rstrip().rstrip(";")
    preview_query = f"{query_text}\nLIMIT 20;"
    return preview_query, engine.execute_query(preview_query, params=None)


def call_airflow_schedule_api(method: str, endpoint: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    airflow_api_url = os.getenv("AIRFLOW_API_URL", "").strip().rstrip("/")
    if not airflow_api_url:
        return {
            "ok": False,
            "skipped": True,
            "message": "AIRFLOW_API_URL is not configured. Schedule metadata was saved locally only.",
        }

    url = f"{airflow_api_url}{endpoint}"
    request_data = json.dumps(payload).encode("utf-8") if payload is not None else None
    api_request = request.Request(url, data=request_data, method=method.upper())
    api_request.add_header("Content-Type", "application/json")

    airflow_username = os.getenv("AIRFLOW_API_USERNAME", "").strip()
    airflow_password = os.getenv("AIRFLOW_API_PASSWORD", "").strip()
    if airflow_username and airflow_password:
        encoded_token = base64.b64encode(f"{airflow_username}:{airflow_password}".encode("utf-8")).decode("utf-8")
        api_request.add_header("Authorization", f"Basic {encoded_token}")

    try:
        with request.urlopen(api_request, timeout=20) as response:
            response_body = response.read().decode("utf-8")
            parsed_body = json.loads(response_body) if response_body else {}
            return {"ok": True, "status": response.status, "data": parsed_body}
    except error.HTTPError as http_error:
        error_body = http_error.read().decode("utf-8", errors="ignore")
        return {"ok": False, "status": http_error.code, "message": error_body or str(http_error)}
    except Exception as exc:
        return {"ok": False, "message": str(exc)}


def create_schedule_payload(
    template_name: str,
    sheet_id: str,
    spreadsheet_title: str,
    cron_expression: str,
) -> dict[str, Any]:
    dag_id = build_dag_id(template_name, sheet_id)
    created_at = datetime.datetime.now().isoformat(timespec="seconds")
    return {
        "dag_id": dag_id,
        "template_name": template_name,
        "sheet_id": sheet_id,
        "spreadsheet_title": spreadsheet_title,
        "sheet_name": template_name,
        "spreadsheet_url": build_spreadsheet_url(sheet_id),
        "cron_expression": cron_expression,
        "created_at": created_at,
    }


def sync_schedule_to_airflow(schedule_payload: dict[str, Any]) -> dict[str, Any]:
    return call_airflow_schedule_api("POST", "/report-schedules", schedule_payload)


def delete_schedule_from_airflow(dag_id: str) -> dict[str, Any]:
    return call_airflow_schedule_api("DELETE", f"/report-schedules/{dag_id}")


def render_schedule_detail(schedule_payload: dict[str, Any]):
    detail_rows = [
        {"Field": "DAG Name", "Value": schedule_payload.get("dag_id", "-")},
        {"Field": "Trigger Cron", "Value": schedule_payload.get("cron_expression", "-")},
        {"Field": "Created At", "Value": schedule_payload.get("created_at", "-")},
        {"Field": "Spreadsheet", "Value": schedule_payload.get("spreadsheet_title", "-")},
        {"Field": "Target URL", "Value": schedule_payload.get("spreadsheet_url", "-")},
        {"Field": "Updated Sheet", "Value": schedule_payload.get("sheet_name", "-")},
    ]
    st.dataframe(detail_rows, hide_index=True, use_container_width=True)


def report_manage():
    st.title("Report Manager")
    init_session_state()
    render_page_tutorial()
    render_template_context()

    gdrive_sheet = get_report_list(sheetdrive.main_id)
    refresh_tab, setschadule_tab, delete_schadule_tab = st.tabs(
        ["Refresh Data", "Generate & Set Schedule", "Delete Schedule"]
    )

    with refresh_tab:
        st.markdown("Update worksheet tabs that already match template names.")
        selected_refresh_report_id = render_spreadsheet_selector(gdrive_sheet, "refresh")

        if st.button("Start Update", type="primary", use_container_width=True):
            if not selected_refresh_report_id:
                st.error("Please choose a spreadsheet first.")
            else:
                with st.spinner("Updating matching worksheets..."):
                    spreadsheet = client.open_by_key(selected_refresh_report_id)
                    metadata = spreadsheet.fetch_sheet_metadata()
                    spreadsheet_title = metadata["properties"]["title"]
                    worksheets = [item["properties"]["title"] for item in metadata["sheets"]]

                    update_results: list[dict[str, Any]] = []
                    for worksheet_name in worksheets:
                        saved_query = templatequery.get_query(worksheet_name)
                        if not saved_query:
                            continue

                        updated_data = engine.execute_query(saved_query, params=None)
                        sheetdrive.update_gsheet(
                            spreadsheet_id=selected_refresh_report_id,
                            file_name=spreadsheet_title,
                            dataframe=updated_data,
                            sheet_name=worksheet_name,
                            overwrite=True,
                            open_browser=False,
                        )
                        update_results.append(
                            {
                                "sheet_name": worksheet_name,
                                "query": saved_query,
                                "data": updated_data,
                            }
                        )

                if not update_results:
                    st.error("No worksheet name matches the available templates.")
                else:
                    st.success(f"{len(update_results)} worksheet(s) updated in `{spreadsheet_title}`.")
                    st.markdown(f"[Open spreadsheet]({build_spreadsheet_url(selected_refresh_report_id)})")
                    preview_tabs = st.tabs([item["sheet_name"] for item in update_results])
                    for tab, result in zip(preview_tabs, update_results):
                        with tab:
                            st.dataframe(result["data"].head(20), use_container_width=True)
                            with st.expander("Show Generated SQL"):
                                st.code(result["query"], language="sql")

    with setschadule_tab:
        st.markdown("Generate one template sheet, overwrite it if it already exists, then register the schedule.")
        selected_schedule_report_id = render_spreadsheet_selector(gdrive_sheet, "schedule")

        selected_template_table = st.selectbox(
            "Select Template",
            options=[""] + templatequery.list_templates(),
            key="selected_template",
            placeholder="Template will be shown below",
        )

        if selected_template_table:
            st.caption(templatequery.get_description(selected_template_table))

        schedule_time = st.time_input("Set Time for Data Refresh", datetime.time(9, 0), key="schadule_time")
        cron_time = time_to_cron(minute=schedule_time.minute, hour=schedule_time.hour)
        st.markdown(f"Schedule set for every day at `{schedule_time}` or cron `{cron_time}`.")

        if selected_template_table:
            preview_query, preview_dataframe = preview_template_query(selected_template_table)
            st.markdown("**Data shown is limited to 20 rows**")
            st.dataframe(preview_dataframe, use_container_width=True)
            with st.expander("Show Generated SQL"):
                st.code(preview_query, language="sql")

        if st.button("Generate & Set Schedule", type="primary", use_container_width=True):
            if not selected_schedule_report_id:
                st.error("Please choose the spreadsheet target first.")
            elif not selected_template_table:
                st.error("Please choose one template first.")
            else:
                with st.spinner("Generating sheet and saving schedule..."):
                    spreadsheet = client.open_by_key(selected_schedule_report_id)
                    spreadsheet_title = spreadsheet.fetch_sheet_metadata()["properties"]["title"]

                    sheetdrive.get_or_create_sheet(
                        spreadsheet_id=selected_schedule_report_id,
                        sheet_name=selected_template_table,
                    )

                    generated_data = run_template_query(selected_template_table)
                    sheetdrive.update_gsheet(
                        spreadsheet_id=selected_schedule_report_id,
                        file_name=spreadsheet_title,
                        dataframe=generated_data,
                        sheet_name=selected_template_table,
                        overwrite=True,
                        open_browser=False,
                    )

                    schedule_payload = create_schedule_payload(
                        template_name=selected_template_table,
                        sheet_id=selected_schedule_report_id,
                        spreadsheet_title=spreadsheet_title,
                        cron_expression=cron_time,
                    )
                    airflow_result = sync_schedule_to_airflow(schedule_payload)
                    schedule_payload["airflow_status"] = "synced" if airflow_result.get("ok") else "local_only"
                    schedule_payload["airflow_message"] = airflow_result.get("message", "")
                    upsert_schedule_registry(schedule_payload)

                st.success(
                    f"Sheet `{selected_template_table}` generated in `{spreadsheet_title}` and schedule `{schedule_payload['dag_id']}` saved."
                )
                st.markdown(f"[Open spreadsheet]({schedule_payload['spreadsheet_url']})")
                if airflow_result.get("ok"):
                    st.info("Airflow API call completed successfully.")
                else:
                    st.info(schedule_payload["airflow_message"] or "Schedule metadata was saved locally.")

        st.caption("DAG naming format follows `template_name + sheet_id`.")

    with delete_schadule_tab:
        st.markdown("Select an existing DAG to review its details or remove it.")
        schedules = load_schedule_registry()

        if not schedules:
            st.info("No saved schedule is available yet.")
        else:
            dag_ids = [item["dag_id"] for item in schedules]
            selected_dag_id = st.selectbox("Scheduled DAG", options=dag_ids)
            selected_schedule = next(item for item in schedules if item["dag_id"] == selected_dag_id)

            render_schedule_detail(selected_schedule)
            if st.button("Delete Selected Schedule", type="primary", use_container_width=True):
                airflow_delete_result = delete_schedule_from_airflow(selected_dag_id)
                delete_schedule_registry(selected_dag_id)
                if airflow_delete_result.get("ok"):
                    st.success(f"Schedule `{selected_dag_id}` deleted.")
                else:
                    st.success(f"Schedule `{selected_dag_id}` removed from the local registry.")
                    if airflow_delete_result.get("message"):
                        st.info(airflow_delete_result["message"])
                st.rerun()


if __name__ == "__main__":
    report_manage()
