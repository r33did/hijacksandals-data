

import streamlit as st
st.set_page_config(page_title="Hijack Data", layout="wide")

from dotenv import load_dotenv
import json
import os
from pathlib import Path
import ExtractPage
import ReportManagerPage
from plugin.postgre.QueryBuilder import ReadTemplate



APP_DIR = Path(__file__).resolve().parent
ROOT_DIR = APP_DIR.parent
CONFIG_DIR = APP_DIR / "config"
APP_CONFIG_PATH = CONFIG_DIR / "app_config.json"
ENV_PATH = ROOT_DIR / ".env"
LOGO_PATH = APP_DIR / "img" / "Logo.png"
TEMPLATE_QUERY = ReadTemplate()

load_dotenv(ENV_PATH)

BASE_PAGES = ["Home", "Data Extractor", "Report Generator & Refresher", "Dashboard", "Analyze"]
ADMIN_PAGES = BASE_PAGES + ["Config"]
USR_PAGES = [page for page in BASE_PAGES if page not in ["Dashboard", "Analyze"]]

DEFAULT_CONFIG = {}


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


def apply_runtime_drive_id(drive_id: str):
    ExtractPage.sheetdrive.set_main_id(drive_id)
    ReportManagerPage.sheetdrive.set_main_id(drive_id)


def get_active_users(config: dict):
    users = config.get("users", {})
    return {username: details for username, details in users.items() if isinstance(details, dict)}


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
        apply_runtime_drive_id(drive_id.strip())
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
    
    st.subheader("Add or Update Template Query")
    tab_create_new_template, template_existing, delete_existing = st.tabs(["Tab Create New Template", "Update Existing Data","Delete Existing Template"])
    with tab_create_new_template :
        templatename_new = st.text_input("Input New Template Name",value=None,key="config_new_template")
        query_new = st.text_area("Input New Query",value=None,key="config_new_query",height=120,)
        description_new = st.text_input("Input Description for this Template",value=None,key="config_new_description")

        if st.button("Add New Template",type="primary",disabled=(True if templatename_new is None and query_new is None and description_new is None else False)):
            TEMPLATE_QUERY.upsert_query(
                template_name= st.session_state.config_new_template,
                query = st.session_state.config_new_query,
                description= st.session_state.config_new_description
            )
            st.success(f"Template `{templatename_new}` Added.")


    with template_existing:
        template = st.selectbox("Select Template Query",options=[""]+TEMPLATE_QUERY.list_templates(),key="config_update_template")
        query_old = TEMPLATE_QUERY.get_query(template_name=template)
        description = TEMPLATE_QUERY.get_description(template_name=template)
        if template != None and query_old != None and description != None :
            st.code(f"Query : \n {query_old} \n Description : {description}",language="sql")
        
        query_new = st.text_area("Input New Query",key="config_update_query",height=120,) or query_old
        description_new = st.text_input("Input Description for this Template",key="config_update_description") or description

        if st.button("Update New Template",type="primary",disabled=(True if template=="" and (query_new is None or description_new is None) else False)):
            TEMPLATE_QUERY.upsert_query(
                template_name= st.session_state.config_update_template,
                query = query_new,
                description= description_new
            )
            st.success(f"Template `{template}` Updated.")

    with delete_existing:
        template = st.selectbox("Select Template Query",options=[""]+TEMPLATE_QUERY.list_templates(),key="config_delete_template")
        query_old = TEMPLATE_QUERY.get_query(template_name=template)
        description = TEMPLATE_QUERY.get_description(template_name=template)
        if template != None and query_old != None and description != None :
            st.code(f"Query : \n {query_old} \n Description : {description}",language="sql")

        if st.button("Delete Template",type="primary",disabled=(True if template=="" else False)):
            TEMPLATE_QUERY.delete_query(
                template_name=st.session_state.config_delete_template
            )
            st.success(f"Template `{template}` Deleted.")



if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

app_config = load_app_config()
USERS = get_active_users(app_config)
apply_runtime_drive_id(app_config.get("google_drive", {}).get("main_id", os.getenv("GDRIVE_ID", "")))

if not st.session_state.logged_in and "user" in st.query_params:
    user_param = st.query_params["user"]
    if user_param in USERS:
        st.session_state.logged_in = True
        st.session_state.username = user_param
        st.session_state.role_pages = normalize_pages(USERS[user_param].get("pages", []))
        st.session_state.page = "Home"

if not st.session_state.logged_in:
    login(USERS)
else:
    if st.session_state.username not in USERS:
        st.session_state.logged_in = False
        st.query_params.clear()
        st.rerun()

    st.session_state.role_pages = normalize_pages(USERS[st.session_state.username].get("pages", []))

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
        if st.button("Logout", use_container_width=True):
            st.session_state.logged_in = False
            st.session_state.page = "Home"
            st.query_params.clear()
            st.rerun()

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
