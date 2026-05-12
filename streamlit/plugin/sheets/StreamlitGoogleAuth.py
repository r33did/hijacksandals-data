import os

import streamlit as st

from plugin.sheets.GoogleAuthAuto import (
    delete_pickle_creds,
    get_manual_refresh_help,
    get_token_status,
    load_valid_creds,
)
from plugin.sheets.UploadSheets import (
    build_oauth_state,
    build_user_oauth_authorization_url,
    delete_user_oauth_creds,
    delete_user_oauth_state,
    load_user_oauth_creds,
    save_user_oauth_state,
    sanitize_token_key,
)


SESSION_CREDS_KEY = "google_drive_oauth_creds"
SESSION_TOKEN_KEY = "google_drive_oauth_token_key"
SESSION_NOTICE_KEY = "google_drive_notice"
SESSION_SOURCE_KEY = "google_drive_oauth_source"
GLOBAL_TOKEN_KEY = "__google_auth_auto__"


def ensure_google_drive_session_defaults():
    defaults = {
        SESSION_CREDS_KEY: None,
        SESSION_TOKEN_KEY: None,
        SESSION_NOTICE_KEY: None,
        SESSION_SOURCE_KEY: None,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def get_google_redirect_uri() -> str:
    redirect_uri = os.getenv("GOOGLE_OAUTH_REDIRECT_URI", "http://localhost:8501").strip()
    return redirect_uri.rstrip("/") or "http://localhost:8501"


def get_google_oauth_token_key(username: str | None = None) -> str:
    active_username = username or st.session_state.get("username", "")
    return sanitize_token_key(active_username)


def set_google_drive_creds(creds, token_key: str | None = None):
    ensure_google_drive_session_defaults()
    st.session_state[SESSION_CREDS_KEY] = creds
    st.session_state[SESSION_TOKEN_KEY] = token_key
    st.session_state[SESSION_SOURCE_KEY] = "session"


def clear_google_drive_creds(remove_saved_token: bool = True):
    ensure_google_drive_session_defaults()
    token_key = st.session_state.get(SESSION_TOKEN_KEY)
    source = st.session_state.get(SESSION_SOURCE_KEY)
    st.session_state[SESSION_CREDS_KEY] = None
    st.session_state[SESSION_TOKEN_KEY] = None
    st.session_state[SESSION_SOURCE_KEY] = None
    if not remove_saved_token:
        return
    if source == "installed_app" or token_key == GLOBAL_TOKEN_KEY:
        delete_pickle_creds()
    elif token_key:
        delete_user_oauth_creds(token_key)
        delete_user_oauth_state(token_key)


def restore_google_drive_creds(username: str | None = None):
    ensure_google_drive_session_defaults()
    token_key = get_google_oauth_token_key(username)
    creds = st.session_state.get(SESSION_CREDS_KEY)
    if not creds or not getattr(creds, "valid", False):
        creds = load_user_oauth_creds(token_key)

    if creds and getattr(creds, "valid", False):
        st.session_state[SESSION_CREDS_KEY] = creds
        st.session_state[SESSION_TOKEN_KEY] = token_key
        st.session_state[SESSION_SOURCE_KEY] = "session"
        return creds

    installed_app_creds = load_valid_creds(delete_invalid_token=True)
    if installed_app_creds and getattr(installed_app_creds, "valid", False):
        st.session_state[SESSION_CREDS_KEY] = installed_app_creds
        st.session_state[SESSION_TOKEN_KEY] = GLOBAL_TOKEN_KEY
        st.session_state[SESSION_SOURCE_KEY] = "installed_app"
        return installed_app_creds

    st.session_state[SESSION_CREDS_KEY] = None
    st.session_state[SESSION_TOKEN_KEY] = None
    st.session_state[SESSION_SOURCE_KEY] = None
    return None


def get_google_drive_creds():
    ensure_google_drive_session_defaults()
    creds = st.session_state.get(SESSION_CREDS_KEY)
    if creds and getattr(creds, "valid", False):
        return creds

    username = st.session_state.get("username")
    if not username:
        return None

    return restore_google_drive_creds(username)


def is_google_drive_connected() -> bool:
    return get_google_drive_creds() is not None


def get_google_drive_auth_source() -> str | None:
    ensure_google_drive_session_defaults()
    return st.session_state.get(SESSION_SOURCE_KEY)


def get_google_drive_token_status() -> dict:
    status = get_token_status()
    status["source"] = get_google_drive_auth_source()
    return status


def get_google_drive_refresh_help(headless: bool = True) -> str:
    return get_manual_refresh_help(headless=headless)


def build_google_drive_connect_url(page_name: str, action_name: str = "connect google drive") -> str | None:
    ensure_google_drive_session_defaults()
    username = st.session_state.get("username")
    if not username:
        return None

    redirect_uri = get_google_redirect_uri()
    state = build_oauth_state(username, page_name, action_name)
    token_key = get_google_oauth_token_key(username)
    authorization_url, code_verifier = build_user_oauth_authorization_url(redirect_uri=redirect_uri, state=state)
    save_user_oauth_state(
        token_key,
        {
            "state": state,
            "redirect_uri": redirect_uri,
            "code_verifier": code_verifier,
            "page_name": page_name,
            "action_name": action_name,
        },
    )
    return authorization_url


def render_google_drive_connect_button(
    page_name: str,
    action_name: str = "connect google drive",
    label: str = "Connect Google Drive",
    use_container_width: bool = True,
):
    if is_google_drive_connected():
        return

    authorization_url = build_google_drive_connect_url(page_name=page_name, action_name=action_name)
    if not authorization_url:
        st.warning("Please log in to the app first.")
        return

    st.link_button(label, authorization_url, use_container_width=use_container_width)
