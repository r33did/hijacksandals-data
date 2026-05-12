from pathlib import Path
import pickle
from typing import Any

import os
from dotenv import load_dotenv


from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 8502
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


def resolve_runtime_path(raw_path: str | Path) -> Path:
    normalized = Path(str(raw_path).replace("\\", "/"))
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


def get_client_secret_path() -> Path:
    return resolve_runtime_path(CLIENT_SECRET)


def get_token_path(token_path: str | Path | None = None) -> Path:
    return resolve_runtime_path(token_path or TOKEN_PATH)


def load_pickle_creds(token_path: str | Path | None = None):
    token_file = get_token_path(token_path)
    if not token_file.exists():
        return None

    try:
        with open(token_file, "rb") as f:
            return pickle.load(f)
    except Exception:
        return None


def save_pickle_creds(creds, token_path: str | Path | None = None) -> Path:
    token_file = get_token_path(token_path)
    token_file.parent.mkdir(parents=True, exist_ok=True)
    with open(token_file, "wb") as f:
        pickle.dump(creds, f)
    return token_file


def delete_pickle_creds(token_path: str | Path | None = None) -> bool:
    token_file = get_token_path(token_path)
    if not token_file.exists():
        return False
    token_file.unlink()
    return True


def get_manual_refresh_command() -> str:
    return f"python {BASE_DIR}"


def get_manual_refresh_help(headless: bool = True, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> str:
    login_mode = "run_local_server(open_browser=False)" if headless else "run_local_server(open_browser=True)"
    return (
        "Token Google Drive perlu dibuat atau login ulang dari terminal VM. "
        f"Jalankan `{get_manual_refresh_command()}` agar helper memakai mode `{login_mode}` "
        f"pada `http://{host}:{port}` dan menyimpan token ke `{get_token_path()}`."
    )


def refresh_pickle_creds(
    creds=None,
    token_path: str | Path | None = None,
    persist: bool = True,
    delete_invalid_token: bool = False,
):
    active_creds = creds or load_pickle_creds(token_path)
    if not active_creds:
        return None

    if active_creds.valid and not active_creds.expired:
        return active_creds

    if active_creds.expired and active_creds.refresh_token:
        try:
            active_creds.refresh(Request())
            if persist:
                save_pickle_creds(active_creds, token_path)
            return active_creds
        except RefreshError:
            if delete_invalid_token:
                delete_pickle_creds(token_path)
            return None
        except Exception:
            if delete_invalid_token:
                delete_pickle_creds(token_path)
            return None

    return active_creds if active_creds.valid else None


def load_valid_creds(
    token_path: str | Path | None = None,
    delete_invalid_token: bool = True,
):
    creds = load_pickle_creds(token_path)
    if not creds:
        return None

    if creds.valid and not creds.expired:
        return creds

    refreshed_creds = refresh_pickle_creds(
        creds=creds,
        token_path=token_path,
        persist=True,
        delete_invalid_token=delete_invalid_token,
    )
    if refreshed_creds and refreshed_creds.valid:
        return refreshed_creds

    if delete_invalid_token:
        delete_pickle_creds(token_path)
    return None


def get_token_status(token_path: str | Path | None = None) -> dict[str, Any]:
    resolved_token_path = get_token_path(token_path)
    status = {
        "connected": False,
        "needs_reauth": False,
        "token_exists": resolved_token_path.exists(),
        "token_path": str(resolved_token_path),
        "client_secret_path": str(get_client_secret_path()),
        "message": "",
    }

    creds = load_pickle_creds(token_path)
    if not creds:
        status["needs_reauth"] = not status["token_exists"]
        status["message"] = (
            "Google Drive token belum tersedia."
            if not status["token_exists"]
            else "Google Drive token tidak bisa dibaca."
        )
        return status

    if creds.valid and not creds.expired:
        status["connected"] = True
        status["message"] = "Google Drive token aktif."
        return status

    if creds.expired and creds.refresh_token:
        refreshed_creds = refresh_pickle_creds(
            creds=creds,
            token_path=token_path,
            persist=True,
            delete_invalid_token=True,
        )
        if refreshed_creds and refreshed_creds.valid:
            status["connected"] = True
            status["message"] = "Google Drive token berhasil di-refresh."
            return status

        status["needs_reauth"] = True
        status["message"] = "Google Drive token expired atau refresh token sudah tidak valid."
        return status

    status["needs_reauth"] = True
    status["message"] = "Google Drive token tidak valid dan perlu login ulang."
    return status


def print_headless_tunnel_help(host: str, port: int):
    print("Open the Google authorization URL from a browser that can reach the callback below:")
    print(f"  http://{host}:{port}/")
    print("If the VM is headless, create an SSH tunnel from your laptop first, for example:")
    print(f"  ssh -L {port}:localhost:{port} user@your-vm-host")
    print("Then open the printed Google URL from your local browser.")


def connect_or_refresh_token(
    headless: bool = True,
    force_reauth: bool = False,
    token_path: str | Path | None = None,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
):
    if not force_reauth:
        creds = load_valid_creds(token_path=token_path, delete_invalid_token=True)
        if creds:
            return creds

    flow = InstalledAppFlow.from_client_secrets_file(str(get_client_secret_path()), SCOPES)
    if not hasattr(flow, "run_local_server"):
        raise RuntimeError("InstalledAppFlow.run_local_server() is not available in this environment.")

    if headless:
        print_headless_tunnel_help(host=host, port=port)
        creds = flow.run_local_server(
            host=host,
            port=port,
            open_browser=False,
            authorization_prompt_message="Please open this URL to authorize this application: {url}",
        )
    else:
        creds = flow.run_local_server(
            host=host,
            port=port,
            open_browser=True,
            authorization_prompt_message="Please open this URL to authorize this application: {url}",
        )

    save_pickle_creds(creds, token_path=token_path)
    return creds


def main(
    headless: bool = True,
    force_reauth: bool = False,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
):
    return connect_or_refresh_token(
        headless=headless,
        force_reauth=force_reauth,
        token_path=TOKEN_PATH,
        host=host,
        port=port,
    )


if __name__ == "__main__":
    main()
