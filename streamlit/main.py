

import streamlit as st
st.set_page_config(page_title="Hijack Data", layout="wide")

from dotenv import load_dotenv
import json
import os
from pathlib import Path
import yaml
import ExtractPage
import ReportManagerPage
from plugin.postgre.QueryBuilder import ReadTemplate
from plugin.sheets.StreamlitGoogleAuth import (
    SESSION_NOTICE_KEY,
    clear_google_drive_creds,
    build_google_drive_connect_url,
    ensure_google_drive_session_defaults,
    get_google_drive_auth_source,
    get_google_drive_refresh_help,
    get_google_drive_token_status,
    get_google_oauth_token_key,
    get_google_redirect_uri,
    restore_google_drive_creds,
    set_google_drive_creds,
)
from plugin.sheets.UploadSheets import (
    build_authorization_response_url,
    delete_user_oauth_state,
    exchange_user_oauth_code,
    load_user_oauth_state,
    parse_oauth_state,
    save_user_oauth_creds,
)



APP_DIR = Path(__file__).resolve().parent
ROOT_DIR = APP_DIR.parent
CONFIG_DIR = APP_DIR / "config"
GENERATED_CONFIG_DIR = ROOT_DIR / "config"
GENERATED_DAGS_DIR = ROOT_DIR / "dags"
APP_CONFIG_PATH = CONFIG_DIR / "app_config.json"
ENV_PATH = ROOT_DIR / ".env"
LOGO_PATH = APP_DIR / "img" / "Logo.png"
TEMPLATE_QUERY = ReadTemplate()

load_dotenv(ENV_PATH)

BASE_PAGES = ["Home", "Data Extractor", "Report Generator & Refresher", "Dashboard", "Analyze"]
ADMIN_PAGES = BASE_PAGES + ["Config"]
USR_PAGES = [page for page in BASE_PAGES if page not in ["Dashboard", "Analyze"]]

DEFAULT_CONFIG = {}


def parse_dags_refresh_input(raw_value: str):
    if not raw_value.strip():
        return []

    try:
        parsed_value = yaml.safe_load(raw_value)
    except yaml.YAMLError:
        separators_normalized = raw_value.replace(",", "\n")
        parsed_value = [item.strip() for item in separators_normalized.splitlines() if item.strip()]

    return TEMPLATE_QUERY.normalize_dags_refresh_items(parsed_value)


def format_dags_refresh_items(items) -> str:
    if not items:
        return ""
    return yaml.safe_dump(items, sort_keys=False).strip()


def render_template_yaml_preview(template_name: str, table_name: str, description: str, dags_refresh_items):
    preview_payload = {
        template_name or "report_name": {
            "description": description or "Template description",
            "table": table_name or "target_table_name",
            "dags_refresh": dags_refresh_items or [
                {
                    "dag_id": "dealpos_fact_inventory_hourly",
                    "loader_key": "fact_inventory",
                    "external_task_id": "load_fact_inventory",
                }
            ],
        }
    }
    st.code(yaml.safe_dump(preview_payload, sort_keys=False), language="yaml")


@st.cache_data(ttl=300)
def get_available_template_views():
    return TEMPLATE_QUERY.list_available_views()


def list_deletable_report_dags():
    if not GENERATED_DAGS_DIR.exists():
        return []

    return sorted(
        [
            dag_path
            for dag_path in GENERATED_DAGS_DIR.glob("*.py")
            if "report" in dag_path.name.lower()
        ],
        key=lambda dag_path: dag_path.name.lower(),
    )


def get_related_report_config_path(dag_path: Path):
    config_stem = dag_path.stem.removeprefix("gen_")
    return GENERATED_CONFIG_DIR / f"{config_stem}.yaml"


def delete_report_dag_artifacts(dag_filename: str):
    selected_dag_path = next(
        (dag_path for dag_path in list_deletable_report_dags() if dag_path.name == dag_filename),
        None,
    )
    if selected_dag_path is None:
        return None, None

    related_config_path = get_related_report_config_path(selected_dag_path)
    had_related_config = related_config_path.exists()
    selected_dag_path.unlink(missing_ok=True)
    if had_related_config:
        related_config_path.unlink()

    return selected_dag_path, related_config_path if had_related_config else None


def ensure_app_config():
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    if not APP_CONFIG_PATH.exists():
        with open(APP_CONFIG_PATH, "w", encoding="utf-8") as config_file:
            json.dump(DEFAULT_CONFIG, config_file, indent=2)


def load_app_config():
    ensure_app_config()
    with open(APP_CONFIG_PATH, "r", encoding="utf-8") as config_file:
        return json.load(config_file)


def save_app_config(config: dict):
    ensure_app_config()
    with open(APP_CONFIG_PATH, "w", encoding="utf-8") as config_file:
        json.dump(config, config_file, indent=2)


def update_env_value(key: str, value: str):
    ENV_PATH.parent.mkdir(parents=True, exist_ok=True)
    env_lines = ENV_PATH.read_text(encoding="utf-8").splitlines() if ENV_PATH.exists() else []
    updated = False

    for index, line in enumerate(env_lines):
        stripped_line = line.strip()
        if not stripped_line or stripped_line.startswith("#") or "=" not in stripped_line:
            continue
        current_key, _, _ = stripped_line.partition("=")
        if current_key.strip() == key:
            env_lines[index] = f"{key} = {value}"
            updated = True
            break

    if not updated:
        env_lines.append(f"{key} = {value}")

    ENV_PATH.write_text("\n".join(env_lines).strip() + "\n", encoding="utf-8")


def normalize_pages(pages):
    return [page for page in pages if page in ADMIN_PAGES] or ["Home"]


def apply_runtime_drive_context(drive_id: str):
    ExtractPage.configure_sheetdrive(folder_id=drive_id)
    ReportManagerPage.configure_sheetdrive(folder_id=drive_id)


def get_active_users(config: dict):
    users = config.get("users", {})
    return {username: details for username, details in users.items() if isinstance(details, dict)}


def handle_google_drive_oauth_callback(users: dict):
    callback_keys = {"code", "state", "scope", "error", "authuser", "prompt", "hd"}
    if not any(key in st.query_params for key in callback_keys):
        return

    state_value = st.query_params.get("state")
    state_payload = parse_oauth_state(state_value)
    username = str(state_payload.get("username", "")).strip()
    target_page = str(state_payload.get("page", "")).strip() or "Home"
    action_name = str(state_payload.get("action", "")).strip() or "Google Drive action"

    if username and username in users:
        st.session_state.logged_in = True
        st.session_state.username = username
        st.session_state.role_pages = normalize_pages(users[username].get("pages", []))
        st.session_state.page = target_page if target_page in ADMIN_PAGES else "Home"

    if "error" in st.query_params:
        error_value = st.query_params.get("error")
        st.session_state[SESSION_NOTICE_KEY] = f"Google authorization was not completed: `{error_value}`."
        st.query_params.clear()
        if username:
            st.query_params["user"] = username
        st.rerun()

    if not username or username not in users or not state_value:
        st.session_state[SESSION_NOTICE_KEY] = "Google authorization callback could not be matched to an active app user."
        st.query_params.clear()
        st.rerun()

    redirect_uri = get_google_redirect_uri()
    callback_params = {key: st.query_params.get(key) for key in callback_keys if key in st.query_params}
    authorization_response = build_authorization_response_url(redirect_uri, callback_params)
    token_key = get_google_oauth_token_key(username)
    oauth_state_payload = load_user_oauth_state(token_key) or {}

    try:
        creds = exchange_user_oauth_code(
            redirect_uri=str(oauth_state_payload.get("redirect_uri") or redirect_uri),
            state=state_value,
            authorization_response=authorization_response,
            code_verifier=oauth_state_payload.get("code_verifier"),
        )
        save_user_oauth_creds(token_key, creds)
        set_google_drive_creds(creds, token_key)
    except Exception as exc:
        clear_google_drive_creds(remove_saved_token=False)
        st.session_state[SESSION_NOTICE_KEY] = f"Google authorization failed: {exc}"
    else:
        delete_user_oauth_state(token_key)
        st.session_state[SESSION_NOTICE_KEY] = (
            f"Google Drive connected for `{action_name}`. "
            "You can continue using the current page."
        )

    st.query_params.clear()
    st.query_params["user"] = username
    st.rerun()


def login(users: dict):
    st.title("Login")
    with st.form("login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submit = st.form_submit_button("Login")

        if submit:
            if username in users and users[username]["password"] == password:
                st.session_state.logged_in = True
                st.session_state.username = username
                st.session_state.role_pages = normalize_pages(users[username].get("pages", []))
                st.session_state.page = "Home"
                st.query_params["user"] = username
                st.success("Login successful!")
                st.rerun()
            else:
                st.error("Invalid username or password")


def render_home_page():
    st.title("Home")
    with st.expander("How this page works", expanded=True):
        st.markdown(
            "\n".join(
                [
                    "Use the left sidebar to move between the available tools.",
                    "Data Extractor is for one-off pulls from PostgreSQL.",
                    "Report Generator & Refresher is for pushing template output into Google Sheets and managing schedules.",
                    "Config is for maintaining users and the shared Drive parent folder.",
                ]
            )
        )

    st.write(f"Welcome, **{st.session_state.username}**.")
    overview_rows = [
        {
            "Page": "Data Extractor",
            "Purpose": "Build a filtered table extract, review SQL, then download or upload the result.",
        },
        {
            "Page": "Report Generator & Refresher",
            "Purpose": "Refresh matching report tabs, generate templated sheets, and manage schedule metadata.",
        },
    ]
    if "Config" in st.session_state.role_pages:
        overview_rows.append(
            {
                "Page": "Config",
                "Purpose": "Manage app users and the Google Drive parent folder used by the app.",
            }
        )

    st.dataframe(overview_rows, hide_index=True, use_container_width=True)


def render_config_page():
    st.title("Configuration")
    with st.expander("How this page works", expanded=True):
        st.markdown(
            "\n".join(
                [
                    "Use this page to manage app users and the main Google Drive folder ID.",
                    "Saving the Drive ID updates both the app config JSON and `.env` for compatibility with the existing helpers.",
                    "Saving a user with an existing username will update that user.",
                ]
            )
        )

    config = load_app_config()
    users = get_active_users(config)

    st.subheader("Current Users")
    user_rows = [
        {
            "username": username,
            "pages": ", ".join(normalize_pages(details.get("pages", []))),
        }
        for username, details in users.items()
    ]
    st.dataframe(user_rows, hide_index=True, use_container_width=True)

    st.markdown("---")

    st.subheader("Google Drive Parent")
    st.caption(f"Google OAuth redirect URI: `{get_google_redirect_uri()}`")
    with st.form("drive_config_form"):
        drive_id = st.text_input(
            "Main Drive ID",
            value=config.get("google_drive", {}).get("main_id", ""),
            help="This folder becomes the parent folder for spreadsheet operations.",
        )
        save_drive = st.form_submit_button("Save Drive Configuration", type="primary")

    if save_drive:
        config.setdefault("google_drive", {})["main_id"] = drive_id.strip()
        save_app_config(config)
        update_env_value("GDRIVE_ID", drive_id.strip())
        apply_runtime_drive_context(drive_id.strip())
        st.success("Drive configuration updated.")
    
    st.markdown("---")

    st.subheader("Add or Update User")
    with st.form("user_config_form"):
        username = st.text_input("Username", placeholder="new.user")
        password = st.text_input("Password", type="password")
        selected_pages = st.multiselect("Allowed Pages", options=ADMIN_PAGES, default=USR_PAGES)
        save_user = st.form_submit_button("Save User", type="primary")

    if save_user:
        if not username.strip() or not password:
            st.error("Username and password are required.")
        else:
            config.setdefault("users", {})[username.strip()] = {
                "password": password,
                "pages": normalize_pages(selected_pages),
            }
            save_app_config(config)
            if st.session_state.username == username.strip():
                st.session_state.role_pages = normalize_pages(selected_pages)
            st.success(f"User `{username.strip()}` saved.")

    deletable_users = [username for username in users.keys() if username != st.session_state.username]
    selected_delete_user = st.selectbox("Delete User", options=[""] + deletable_users)
    if st.button("Delete Selected User", disabled=not selected_delete_user):
        config["users"].pop(selected_delete_user, None)
        save_app_config(config)
        st.success(f"User `{selected_delete_user}` deleted.")
        st.rerun()

    st.markdown("---")
    
    st.subheader("Add or Update Template Config")
    tab_create_new_template, template_existing, delete_existing = st.tabs(
        ["Create Template", "Update Existing Data", "Delete Existing Template"]
    )
    default_dags_refresh_text = yaml.safe_dump(
        [
            {
                "dag_id": "dealpos_fact_invoice_line_hourly",
                "loader_key": "fact_invoice_line",
                "external_task_id": "load_fact_invoice_line",
            },
            {
                "dag_id": "dealpos_variant_data_hourly",
                "loader_key": "variant_data",
                "external_task_id": "load_variant_data",
            },
        ],
        sort_keys=False,
    ).strip()

    with tab_create_new_template :
        available_template_views = get_available_template_views()
        templatename_new = st.text_input("Input New Template Name",value=None,key="config_new_template")
        table_new = st.selectbox(
            "Select Source View",
            options=[""] + available_template_views,
            key="config_new_table",
            help="Only views from information_schema for the current app schema are shown here.",
        )
        if available_template_views:
            st.caption(f"{len(available_template_views)} view(s) available from the database schema.")
        else:
            st.warning("No source views were found from information_schema. Check the database connection or schema.")
        description_new = st.text_input("Input Description for this Template",value=None,key="config_new_description")
        dags_refresh_new = st.text_area(
            "Input DAG Refresh List",
            value=default_dags_refresh_text,
            key="config_new_dags_refresh",
            height=180,
            help="Use YAML list items with dag_id / loader_key / external_task_id.",
        )
        parsed_new_dags_refresh = parse_dags_refresh_input(dags_refresh_new) if dags_refresh_new else []
        st.caption("Template YAML preview")
        render_template_yaml_preview(
            templatename_new or "",
            table_new or "",
            description_new or "",
            parsed_new_dags_refresh,
        )

        if st.button(
            "Add New Template",
            type="primary",
            disabled=(True if not templatename_new or not table_new else False),
        ):
            TEMPLATE_QUERY.upsert_template(
                template_name= st.session_state.config_new_template,
                table=st.session_state.config_new_table,
                description= st.session_state.config_new_description,
                dags_refresh=parse_dags_refresh_input(st.session_state.config_new_dags_refresh),
            )
            st.success(f"Template `{templatename_new}` Added.")


    with template_existing:
        template = st.selectbox("Select Template Query",options=[""]+TEMPLATE_QUERY.list_templates(),key="config_update_template")
        table_old = TEMPLATE_QUERY.get_table(template_name=template)
        description = TEMPLATE_QUERY.get_description(template_name=template)
        dags_refresh_old = TEMPLATE_QUERY.get_dags_refresh_items(template_name=template)
        dags_refresh_old_text = format_dags_refresh_items(dags_refresh_old)
        if st.session_state.get("config_update_template_loaded") != template:
            st.session_state["config_update_template_loaded"] = template
            st.session_state["config_update_table"] = table_old or ""
            st.session_state["config_update_description"] = description or ""
            st.session_state["config_update_dags_refresh"] = dags_refresh_old_text
        if template and (table_old or TEMPLATE_QUERY.get_query(template_name=template)):
            st.code(
                "\n".join(
                    [
                        f"Table : {table_old or '-'}",
                        f"Description : {description}",
                        "DAG Refresh :",
                        yaml.safe_dump(dags_refresh_old, sort_keys=False).strip() if dags_refresh_old else "-",
                    ]
                ),
                language="yaml",
            )
        
        table_new = st.text_input("Input Source Table",key="config_update_table") or table_old
        description_new = st.text_input("Input Description for this Template",key="config_update_description") or description
        dags_refresh_new = st.text_area(
            "Input DAG Refresh List",
            key="config_update_dags_refresh",
            height=180,
            placeholder="- dag_id: dealpos_fact_inventory_hourly\n  loader_key: fact_inventory\n  external_task_id: load_fact_inventory",
        )
        selected_dags_refresh = parse_dags_refresh_input(dags_refresh_new) if dags_refresh_new else dags_refresh_old
        st.caption("Updated template YAML preview")
        render_template_yaml_preview(
            template or "",
            table_new or "",
            description_new or "",
            selected_dags_refresh,
        )

        if st.button("Update Template",type="primary",disabled=(True if template=="" or not table_new else False)):
            TEMPLATE_QUERY.upsert_template(
                template_name= st.session_state.config_update_template,
                table=table_new,
                description= description_new,
                dags_refresh=selected_dags_refresh,
            )
            st.success(f"Template `{template}` Updated.")

    with delete_existing:
        template = st.selectbox("Select Template Query",options=[""]+TEMPLATE_QUERY.list_templates(),key="config_delete_template")
        table_old = TEMPLATE_QUERY.get_table(template_name=template)
        description = TEMPLATE_QUERY.get_description(template_name=template)
        dags_refresh_old = TEMPLATE_QUERY.get_dags_refresh_items(template_name=template)
        if template and (table_old or TEMPLATE_QUERY.get_query(template_name=template)):
            st.code(
                "\n".join(
                    [
                        f"Table : {table_old or '-'}",
                        f"Description : {description}",
                        "DAG Refresh :",
                        yaml.safe_dump(dags_refresh_old, sort_keys=False).strip() if dags_refresh_old else "-",
                    ]
                ),
                language="yaml",
            )

        if st.button("Delete Template",type="primary",disabled=(True if template=="" else False)):
            TEMPLATE_QUERY.delete_query(
                template_name=st.session_state.config_delete_template
            )
            st.success(f"Template `{template}` Deleted.")

        st.divider()
        st.subheader("Delete Generated Report DAG")
        deletable_report_dags = list_deletable_report_dags()
        selected_report_dag = st.selectbox(
            "Select Generated Report DAG",
            options=[""] + [dag_path.name for dag_path in deletable_report_dags],
            key="config_delete_report_dag",
        )
        if selected_report_dag:
            selected_report_dag_path = GENERATED_DAGS_DIR / selected_report_dag
            related_report_config_path = get_related_report_config_path(selected_report_dag_path)
            st.code(
                "\n".join(
                    [
                        f"DAG File : {selected_report_dag_path}",
                        f"Related YAML : {related_report_config_path if related_report_config_path.exists() else '-'}",
                    ]
                ),
                language="text",
            )
        elif not deletable_report_dags:
            st.caption("No generated DAG files containing `report` were found in `/dags`.")

        if st.button("Delete Selected Report DAG", type="primary", disabled=not selected_report_dag):
            deleted_dag_path, deleted_config_path = delete_report_dag_artifacts(selected_report_dag)
            if deleted_dag_path is None:
                st.error("Selected DAG is no longer available.")
            else:
                deleted_paths = [str(deleted_dag_path)]
                if deleted_config_path is not None:
                    deleted_paths.append(str(deleted_config_path))
                st.success("Deleted:\n" + "\n".join(deleted_paths))
                st.rerun()



if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

ensure_google_drive_session_defaults()
app_config = load_app_config()
USERS = get_active_users(app_config)
handle_google_drive_oauth_callback(USERS)

if not st.session_state.logged_in and "user" in st.query_params:
    user_param = st.query_params["user"]
    if user_param in USERS:
        st.session_state.logged_in = True
        st.session_state.username = user_param
        st.session_state.role_pages = normalize_pages(USERS[user_param].get("pages", []))
        st.session_state.page = "Home"

if not st.session_state.logged_in:
    clear_google_drive_creds(remove_saved_token=False)
    login(USERS)
else:
    if st.session_state.username not in USERS:
        st.session_state.logged_in = False
        clear_google_drive_creds(remove_saved_token=False)
        st.query_params.clear()
        st.rerun()

    st.session_state.role_pages = normalize_pages(USERS[st.session_state.username].get("pages", []))
    runtime_google_creds = restore_google_drive_creds(st.session_state.username)
    apply_runtime_drive_context(
        app_config.get("google_drive", {}).get("main_id", os.getenv("GDRIVE_ID", "")),
    )

    with st.sidebar:
        col1, col2, col3 = st.columns([1, 50, 1])
        with col2:
            if LOGO_PATH.exists():
                st.image(str(LOGO_PATH), width=200, use_container_width=True)

        st.markdown("## Navigation")

        allowed_pages = st.session_state.role_pages
        for page in allowed_pages:
            if st.button(
                page,
                key=f"nav_{page}",
                use_container_width=True,
                type="primary" if st.session_state.page == page else "tertiary",
            ):
                st.session_state.page = page
                st.rerun()

        st.markdown("---")
        st.write(f"Logged in as: **{st.session_state.username}**")
        token_status = get_google_drive_token_status()
        auth_source = get_google_drive_auth_source()
        if runtime_google_creds:
            source_label = "Installed Flow token" if auth_source == "installed_app" else "Web OAuth token"
            st.write(f"Google Drive: **Connected**")
            st.caption(f"Credential source: {source_label}.")
        else:
            st.write("Google Drive: **Not connected**")
            if token_status.get("message"):
                st.caption(token_status["message"])

        if not runtime_google_creds:
            if st.button("Check Google Drive Token", use_container_width=True):
                runtime_google_creds = restore_google_drive_creds(st.session_state.username)
                if runtime_google_creds:
                    st.session_state[SESSION_NOTICE_KEY] = "Google Drive token ditemukan dan siap dipakai."
                else:
                    status_message = token_status.get("message") or "Google Drive token belum tersedia."
                    st.session_state[SESSION_NOTICE_KEY] = (
                        f"{status_message} {get_google_drive_refresh_help(headless=True)}"
                    )
                st.rerun()

            with st.expander("How to refresh Google Drive token", expanded=False):
                st.markdown(get_google_drive_refresh_help(headless=True))
                if token_status.get("token_path"):
                    st.code(token_status["token_path"], language="text")

            google_connect_url = build_google_drive_connect_url(
                page_name=st.session_state.page if "page" in st.session_state else "Home",
                action_name="connect google drive",
            )
            if google_connect_url:
                st.caption("Optional: you can still use the existing web OAuth flow below.")
                st.link_button("Connect Google Drive (Web OAuth)", google_connect_url, use_container_width=True)
        if runtime_google_creds and st.button("Disconnect Google Drive", use_container_width=True,disabled=True): # False apabila sudah bener 
            clear_google_drive_creds()
            disconnect_scope = "the shared Google Drive token" if auth_source == "installed_app" else "this app user"
            st.session_state[SESSION_NOTICE_KEY] = f"Google Drive connection removed for {disconnect_scope}."
            st.rerun()
        if st.button("Logout", use_container_width=True):
            st.session_state.logged_in = False
            st.session_state.page = "Home"
            clear_google_drive_creds(remove_saved_token=False)
            st.query_params.clear()
            st.rerun()

    if st.session_state.get(SESSION_NOTICE_KEY):
        st.info(st.session_state[SESSION_NOTICE_KEY])
        st.session_state[SESSION_NOTICE_KEY] = None

    selection = st.session_state.page if "page" in st.session_state else "Home"

    if selection == "Home":
        render_home_page()
    elif selection == "Data Extractor":
        ExtractPage.extract_page()
    elif selection == "Report Generator & Refresher":
        ReportManagerPage.report_manage()
    elif selection == "Dashboard":
        st.title("Dashboard")
    elif selection == "Analyze":
        st.title("Analyze")
    elif selection == "Config":
        render_config_page()
