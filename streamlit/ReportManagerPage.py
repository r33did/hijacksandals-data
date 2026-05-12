import datetime
import json
import runpy
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from plugin.postgre.QueryBuilder import Engine, ReadTemplate
from plugin.sheets.StreamlitGoogleAuth import (
    get_google_drive_creds,
    is_google_drive_connected,
)
from plugin.sheets.UploadSheets import sheetdrive as SheetDrive
from plugin.create_yaml.yaml_creator import create_dags_yaml

engine = Engine()
sheetdrive = SheetDrive()
client = sheetdrive.connect_gspread()
templatequery = ReadTemplate()

BASE_DIR = Path(__file__).resolve()
ROOT_DIR = BASE_DIR.parents[1]

# CONFIG_DIR = BASE_DIR.parent / "config" -> ROOT_DIR / "config" pake config di ROOT_DIR aja, karena generate_dags.py juga butuh akses ke config dan dijalankan dari ROOT_DIR
CONFIG_DIR = ROOT_DIR / "config"
BLUEPRINT_DIR = ROOT_DIR / "streamlit" / "config" / "sheet_blueprints"
SCHEDULE_REGISTRY_PATH = CONFIG_DIR / "scheduled_reports.json"
ENV_PATH = ROOT_DIR / ".env"

load_dotenv(ENV_PATH)


def configure_sheetdrive(creds=None, folder_id: str | None = None):
    global sheetdrive, client
    sheetdrive = SheetDrive(creds=creds)
    if folder_id:
        sheetdrive.set_main_id(folder_id)
    client = sheetdrive.connect_gspread()

REPORT_MANAGER_SECTIONS = [
    "Refresh Data",
    "Generate & Set Schedule",
    "Delete Schedule",
]
SCHEDULE_TARGET_MODES = [
    "Copy from Template Drive",
    "Create from Blueprint Template",
    "Use Existing Spreadsheet ID",
]
SCHEDULE_DESTINATION_MODES = [
    "Default Folder Directory",
    "Manual Drive Folder ID",
]


@st.cache_data(ttl=300)
def get_report_list(folder_id: str, auth_cache_key: str = "service_account"):
    _ = auth_cache_key
    return dict(sheetdrive.list_folder(folder_id=folder_id))


@st.cache_data(ttl=300)
def get_template_report_list(auth_cache_key: str = "service_account"):
    _ = auth_cache_key
    return dict(sheetdrive.list_folder(folder_id=sheetdrive.template_id, include=".*"))


@st.cache_data(ttl=300)
def get_blueprint_template_list() -> dict[str, str]:
    if not BLUEPRINT_DIR.exists():
        return {}
    return {
        blueprint_path.name: str(blueprint_path)
        for blueprint_path in sorted(BLUEPRINT_DIR.glob("*.json"))
    }


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
        "schedule_target_mode": SCHEDULE_TARGET_MODES[0],
        "schedule_destination_mode": SCHEDULE_DESTINATION_MODES[0],
        "report_manager_active_section": REPORT_MANAGER_SECTIONS[0],
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def set_active_report_section(section_name: str):
    st.session_state["report_manager_active_section"] = section_name


def get_report_section_order() -> list[str]:
    active_section = st.session_state.get("report_manager_active_section", REPORT_MANAGER_SECTIONS[0])
    if active_section not in REPORT_MANAGER_SECTIONS:
        active_section = REPORT_MANAGER_SECTIONS[0]
    return [active_section] + [section for section in REPORT_MANAGER_SECTIONS if section != active_section]


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
        columns={
            "template_name": "Template Name",
            "description": "Description",
            "table": "Table",
            "dags_refresh": "DAGs Refresh",
        }
    )


def render_page_tutorial():
    with st.expander("How this page works", expanded=True):
        st.markdown(
            "\n".join(
                [
                    "1. `Refresh Data` updates worksheet tabs whose names already match saved templates.",
                    "2. `Generate & Set Schedule` creates or overwrites one selected sheet, writes a DAG YAML config, then runs the DAG generator.",
                    "3. `Delete Schedule` lists saved DAGs, shows their details, and removes the local registry plus generated files.",
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
                on_change=set_active_report_section,
                args=("Refresh Data" if state_prefix == "refresh" else "Generate & Set Schedule",),
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
                    on_change=set_active_report_section,
                    args=("Refresh Data" if state_prefix == "refresh" else "Generate & Set Schedule",),
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
            on_change=set_active_report_section,
            args=("Refresh Data" if state_prefix == "refresh" else "Generate & Set Schedule",),
        )

    return selected_report_id


def render_schedule_destination_folder_selector() -> str | None:
    destination_mode = st.radio(
        "Destination Folder",
        options=SCHEDULE_DESTINATION_MODES,
        horizontal=True,
        key="schedule_destination_mode",
        on_change=set_active_report_section,
        args=("Generate & Set Schedule",),
    )

    if destination_mode == SCHEDULE_DESTINATION_MODES[0]:
        default_folder_id = (sheetdrive.main_id or "").strip()
        if default_folder_id:
            st.caption(f"Using default folder ID: `{default_folder_id}`")
            return default_folder_id
        st.warning("Default Drive folder ID is not configured yet.")
        return None

    manual_folder_id = st.text_input(
        "Input Drive Folder ID",
        placeholder="Folder ID from Google Drive",
        key="schedule_destination_folder_id",
        on_change=set_active_report_section,
        args=("Generate & Set Schedule",),
    )
    return (manual_folder_id or "").strip() or None


def run_template_query(template_name: str) -> pd.DataFrame:
    query_text = templatequery.get_query(template_name)
    if not query_text:
        return pd.DataFrame()
    return engine.execute_query(query_text, params=None)


def preview_template_query(template_name: str) -> tuple[str, pd.DataFrame]:
    table_name = templatequery.get_table(template_name)
    if not table_name:
        return "No table is configured for this template.", pd.DataFrame()
    preview_query = templatequery.build_table_query(table_name, limit=20)
    return preview_query, engine.execute_query(preview_query, params=None)


def create_schedule_payload(
    template_name: str,
    sheet_id: str,
    spreadsheet_title: str,
    cron_expression: str,
    yaml_path: Path,
    generated_dag_path: Path,
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
        "table": templatequery.get_table(template_name),
        "dags_refresh": templatequery.get_dags_refresh(template_name),
        "yaml_path": str(yaml_path),
        "generated_dag_path": str(generated_dag_path),
        "created_at": created_at,
    }


def generate_dag_artifacts(
    template_name: str,
    sheet_id: str,
    spreadsheet_name: str,
    cron_expression: str,
    project: str = "hijack-sandal",
) -> tuple[Path, Path]:
    dag_id = build_dag_id(template_name, sheet_id)
    yaml_path = create_dags_yaml(
        config_dir=CONFIG_DIR,
        file_name=f"{dag_id}.yaml",
        schedule=cron_expression,
        project=project,
        template_name=template_name,
        table=templatequery.get_table(template_name) or "",
        dags_refresh=templatequery.get_dags_refresh_items(template_name),
        description=templatequery.get_description(template_name),
        spreadsheet_id=sheet_id,
        sheet_name = template_name,
        spreadsheet_title=spreadsheet_name,
    )
    runpy.run_path(str(ROOT_DIR / "template" / "generate_dags.py"), run_name="__main__") # Run python func
    generated_dag_path = ROOT_DIR / "dags" / f"gen_{dag_id}.py"
    return yaml_path, generated_dag_path


def remove_generated_schedule_files(schedule_payload: dict[str, Any]) -> list[str]:
    removed_paths: list[str] = []
    for key in ["yaml_path", "generated_dag_path"]:
        raw_path = schedule_payload.get(key)
        if not raw_path:
            continue
        target_path = Path(str(raw_path))
        if target_path.exists():
            target_path.unlink()
            removed_paths.append(str(target_path))
    return removed_paths


def render_schedule_detail(schedule_payload: dict[str, Any]):
    detail_rows = [
        {"Field": "DAG Name", "Value": schedule_payload.get("dag_id", "-")},
        {"Field": "Trigger Cron", "Value": schedule_payload.get("cron_expression", "-")},
        {"Field": "Created At", "Value": schedule_payload.get("created_at", "-")},
        {"Field": "Table", "Value": schedule_payload.get("table", "-")},
        {"Field": "DAG Refresh", "Value": ", ".join(schedule_payload.get("dags_refresh", [])) or "-"},
        {"Field": "Spreadsheet", "Value": schedule_payload.get("spreadsheet_title", "-")},
        {"Field": "Target URL", "Value": schedule_payload.get("spreadsheet_url", "-")},
        {"Field": "Updated Sheet", "Value": schedule_payload.get("sheet_name", "-")},
        {"Field": "YAML Config", "Value": schedule_payload.get("yaml_path", "-")},
        {"Field": "Generated DAG", "Value": schedule_payload.get("generated_dag_path", "-")},
    ]
    st.dataframe(detail_rows, hide_index=True, use_container_width=True)


def report_manage():
    st.title("Report Manager")
    init_session_state()
    render_page_tutorial()
    render_template_context()
    google_drive_connected = is_google_drive_connected()
    st.caption(
        "Google Drive status: "
        + (
            "Connected"
            if google_drive_connected
            else "Not connected. Connect Google Drive first before using `Generate & Set Schedule`."
        )
    )
    auth_cache_key = st.session_state.get("google_drive_oauth_token_key") or "service_account"
    gdrive_sheet = get_report_list(sheetdrive.main_id, auth_cache_key=auth_cache_key)
    ordered_sections = get_report_section_order()
    rendered_tabs = dict(zip(ordered_sections, st.tabs(ordered_sections)))

    with rendered_tabs["Refresh Data"]:
        st.markdown("Update worksheet tabs that already match template names.")
        selected_refresh_report_id = render_spreadsheet_selector(gdrive_sheet, "refresh")

        if st.button(
            "Start Update",
            type="primary",
            use_container_width=True,
            on_click=set_active_report_section,
            args=("Refresh Data",),
        ):
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

    with rendered_tabs["Generate & Set Schedule"]:
        st.markdown("Generate one template sheet, overwrite it if it already exists, then generate the DAG YAML and DAG file.")
        schedule_target_mode = st.radio(
            "Target Method",
            options=SCHEDULE_TARGET_MODES,
            horizontal=True,
            key="schedule_target_mode",
            on_change=set_active_report_section,
            args=("Generate & Set Schedule",),
        )

        selected_schedule_report_id = None
        selected_template_source_name = None
        selected_template_source_id = None
        selected_blueprint_name = None
        selected_blueprint_path = None

        if schedule_target_mode == SCHEDULE_TARGET_MODES[0]:
            template_drive_files = get_template_report_list(auth_cache_key=auth_cache_key)
            template_file_names = sorted(template_drive_files.keys())

            selected_template_source_name = st.selectbox(
                "Select Template Spreadsheet",
                options=template_file_names if template_file_names else ["No template spreadsheet found"],
                disabled=not template_file_names,
                key="schedule_template_source_name",
                on_change=set_active_report_section,
                args=("Generate & Set Schedule",),
            )
            if template_file_names:
                selected_template_source_id = template_drive_files.get(selected_template_source_name)

            st.text_input(
                "Copied Spreadsheet Title",
                placeholder="Leave blank to reuse the template spreadsheet name",
                key="schedule_copied_spreadsheet_name",
                on_change=set_active_report_section,
                args=("Generate & Set Schedule",),
            )
            destination_folder_id = render_schedule_destination_folder_selector()
        elif schedule_target_mode == SCHEDULE_TARGET_MODES[1]:
            blueprint_files = get_blueprint_template_list()
            blueprint_names = sorted(blueprint_files.keys())

            selected_blueprint_name = st.selectbox(
                "Select Blueprint Template",
                options=blueprint_names if blueprint_names else ["No blueprint template found"],
                disabled=not blueprint_names,
                key="schedule_blueprint_source_name",
                on_change=set_active_report_section,
                args=("Generate & Set Schedule",),
            )
            if blueprint_names:
                selected_blueprint_path = blueprint_files.get(selected_blueprint_name)

            st.text_input(
                "Generated Spreadsheet Title",
                placeholder="Leave blank to reuse the blueprint file name",
                key="schedule_blueprint_spreadsheet_name",
                on_change=set_active_report_section,
                args=("Generate & Set Schedule",),
            )
            destination_folder_id = render_schedule_destination_folder_selector()
        else:
            manual_schedule_input = st.text_input(
                "Input Report URL or Spreadsheet ID",
                placeholder="https://docs.google.com/spreadsheets/d/{ID}/edit",
                key="schedule_manual_spreadsheet_input",
                on_change=set_active_report_section,
                args=("Generate & Set Schedule",),
            )
            selected_schedule_report_id = parse_spreadsheet_id(manual_schedule_input)
            if manual_schedule_input and not selected_schedule_report_id:
                st.warning("Spreadsheet ID could not be read from the input.")
            destination_folder_id = None

        selected_template_table = st.selectbox(
            "Select Template",
            options=[""] + templatequery.list_templates(),
            key="selected_template",
            placeholder="Template will be shown below",
            on_change=set_active_report_section,
            args=("Generate & Set Schedule",),
        )

        if selected_template_table:
            st.markdown(f"Description: `{templatequery.get_description(selected_template_table)}`")
            # DEBUG : 
            # st.markdown(f"Table source: `{templatequery.get_table(selected_template_table) or '-'}`")
            # st.markdown(
            #     f"DAG refresh dependencies: `{', '.join(templatequery.get_dags_refresh(selected_template_table)) or '-'}`"
            # )

        schedule_time = st.time_input(
            "Set Time for Data Refresh",
            datetime.time(9, 0),
            key="schadule_time",
            on_change=set_active_report_section,
            args=("Generate & Set Schedule",),
        )
        cron_time = time_to_cron(minute=schedule_time.minute, hour=schedule_time.hour)
        st.markdown(f"Schedule set for every day at `{schedule_time}` or cron `{cron_time}`.")

        if selected_template_table and templatequery.get_table(selected_template_table):
            preview_query, preview_dataframe = preview_template_query(selected_template_table)
            st.markdown("**Data shown is limited to 20 rows**")
            st.dataframe(preview_dataframe, use_container_width=True)
            with st.expander("Show Generated SQL"):
                st.code(preview_query, language="sql")
        elif selected_template_table:
            st.warning("This template does not have a source table yet.")

        if st.button(
            "Generate & Set Schedule",
            type="primary",
            use_container_width=True,
            disabled=not google_drive_connected,
            on_click=set_active_report_section,
            args=("Generate & Set Schedule",),
        ):
            if schedule_target_mode == SCHEDULE_TARGET_MODES[0] and not selected_template_source_id:
                st.error("Please choose one template spreadsheet to copy first.")
            elif schedule_target_mode == SCHEDULE_TARGET_MODES[0] and not destination_folder_id:
                st.error("Please choose a destination Drive folder first.")
            elif schedule_target_mode == SCHEDULE_TARGET_MODES[1] and not selected_blueprint_path:
                st.error("Please choose one blueprint template first.")
            elif schedule_target_mode == SCHEDULE_TARGET_MODES[1] and not destination_folder_id:
                st.error("Please choose a destination Drive folder first.")
            elif schedule_target_mode == SCHEDULE_TARGET_MODES[2] and not selected_schedule_report_id:
                st.error("Please input the spreadsheet target first.")
            elif not selected_template_table:
                st.error("Please choose one template first.")
            elif not templatequery.get_table(selected_template_table):
                st.error("Selected template does not define a source table.")
            else:
                try:
                    with st.spinner("Generating sheet, YAML config, and DAG file..."):
                        authorized_sheetdrive = SheetDrive(creds=get_google_drive_creds())
                        authorized_sheetdrive.set_main_id(sheetdrive.main_id)
                        authorized_client = authorized_sheetdrive.connect_gspread()

                        if schedule_target_mode == SCHEDULE_TARGET_MODES[0]:
                            copied_spreadsheet_name = (st.session_state.get("schedule_copied_spreadsheet_name") or "").strip()
                            selected_schedule_report_id = authorized_sheetdrive.copy_template(
                                file_name=selected_template_source_name,
                                new_title=copied_spreadsheet_name or selected_template_source_name,
                                destination_folder_id=destination_folder_id,
                                template_file_id=selected_template_source_id,
                            )
                        elif schedule_target_mode == SCHEDULE_TARGET_MODES[1]:
                            generated_spreadsheet_name = (st.session_state.get("schedule_blueprint_spreadsheet_name") or "").strip()
                            selected_schedule_report_id = authorized_sheetdrive.create_from_blueprint(
                                blueprint_path=selected_blueprint_path,
                                spreadsheet_name=generated_spreadsheet_name or Path(selected_blueprint_path).stem,
                                destination_folder_id=destination_folder_id,
                            )

                        spreadsheet = authorized_client.open_by_key(selected_schedule_report_id)
                        spreadsheet_title = spreadsheet.fetch_sheet_metadata()["properties"]["title"]

                        authorized_sheetdrive.get_or_create_sheet(
                            spreadsheet_id=selected_schedule_report_id,
                            sheet_name=selected_template_table,
                        )

                        generated_data = run_template_query(selected_template_table)
                        authorized_sheetdrive.update_gsheet(
                            spreadsheet_id=selected_schedule_report_id,
                            file_name=spreadsheet_title,
                            dataframe=generated_data,
                            sheet_name=selected_template_table,
                            overwrite=True,
                            open_browser=False,
                        )

                        yaml_path, generated_dag_path = generate_dag_artifacts(
                            template_name=selected_template_table,
                            sheet_id=selected_schedule_report_id,
                            cron_expression=cron_time,
                            spreadsheet_name=spreadsheet_title,
                        )
                        schedule_payload = create_schedule_payload(
                            template_name=selected_template_table,
                            sheet_id=selected_schedule_report_id,
                            spreadsheet_title=spreadsheet_title,
                            cron_expression=cron_time,
                            yaml_path=yaml_path,
                            generated_dag_path=generated_dag_path,
                        )
                        # upsert_schedule_registry(schedule_payload)
                except Exception as exc:
                    st.error(f"Failed to generate schedule assets: {exc}")
                else:
                    st.success(
                        f"Sheet `{selected_template_table}` generated in `{spreadsheet_title}` and DAG `{schedule_payload['dag_id']}` saved."
                    )
                    st.markdown(f"[Open spreadsheet]({schedule_payload['spreadsheet_url']})")
                    st.info(f"Config file: `{yaml_path.name}`")
                    st.info(f"DAG file: `{generated_dag_path.name}`")

        st.caption("DAG naming format follows `template_name + sheet_id`.")

    with rendered_tabs["Delete Schedule"]:
        st.markdown("Select an existing DAG to review its details or remove it.")
        schedules = load_schedule_registry()

        if not schedules:
            st.info("No saved schedule is available yet.")
        else:
            dag_ids = [item["dag_id"] for item in schedules]
            selected_dag_id = st.selectbox(
                "Scheduled DAG",
                options=dag_ids,
                on_change=set_active_report_section,
                args=("Delete Schedule",),
            )
            selected_schedule = next(item for item in schedules if item["dag_id"] == selected_dag_id)

            render_schedule_detail(selected_schedule)
            if st.button(
                "Delete Selected Schedule",
                type="primary",
                use_container_width=True,
                on_click=set_active_report_section,
                args=("Delete Schedule",),
            ):
                removed_paths = remove_generated_schedule_files(selected_schedule)
                delete_schedule_registry(selected_dag_id)
                st.success(f"Schedule `{selected_dag_id}` removed from the local registry.")
                if removed_paths:
                    st.info("Deleted generated files:\n" + "\n".join(removed_paths))
                st.rerun()


if __name__ == "__main__":
    report_manage()
