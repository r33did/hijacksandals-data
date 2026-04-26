from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml


def normalize_yaml_name(name: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", name.strip())
    normalized = normalized.strip("-.")
    return normalized or "new-config"


def normalize_task_name(name: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9_]+", "_", name.strip())
    normalized = normalized.strip("_")
    return normalized or "task"


def normalize_dags_refresh_items(dags_refresh: list[dict[str, Any]] | None) -> list[dict[str, str]]:
    normalized_items: list[dict[str, str]] = []
    for raw_item in dags_refresh or []:
        if not isinstance(raw_item, dict):
            continue

        dag_id = str(raw_item.get("dag_id", "")).strip()
        loader_key = str(raw_item.get("loader_key", "")).strip()
        external_task_id = str(raw_item.get("external_task_id", "")).strip()

        if not dag_id or not external_task_id:
            continue

        normalized_items.append(
            {
                "dag_id": dag_id,
                "loader_key": loader_key,
                "external_task_id": external_task_id,
            }
        )
    return normalized_items


def create_dags_yaml(
    config_dir: str | Path,
    file_name: str,
    schedule: str,
    project: str,
    template_name: str,
    table: str,
    spreadsheet_id: str,
    spreadsheet_title: str,
    sheet_name: str,
    dags_refresh: list[dict[str, Any]] | None = None,
    description: str | None = None,
) -> Path:
    if not schedule.strip():
        raise ValueError("Schedule is required.")
    if not project.strip():
        raise ValueError("Project is required.")
    if not template_name.strip():
        raise ValueError("Template name is required.")
    if not table.strip():
        raise ValueError("Table is required.")
    if not spreadsheet_id.strip():
        raise ValueError("Spreadsheet ID is required.")
    if not sheet_name.strip():
        raise ValueError("Sheet name is required.")

    dags_refresh_items = normalize_dags_refresh_items(dags_refresh)
    task_prefix = normalize_task_name(template_name)
    output_dir = Path(config_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / normalize_yaml_name(file_name)
    if output_path.suffix.lower() not in {".yaml", ".yml"}:
        output_path = output_path.with_suffix(".yaml")

    tasks: list[dict[str, Any]] = []
    wait_task_names: list[str] = []

    for refresh_item in dags_refresh_items:
        dag_id = refresh_item["dag_id"]
        loader_key = refresh_item.get("loader_key") or dag_id
        external_task_id = refresh_item["external_task_id"]
        trigger_task_name = normalize_task_name(f"{task_prefix}_trigger_{loader_key}")
        wait_task_name = normalize_task_name(f"{task_prefix}_wait_{loader_key}")

        tasks.append(
            {
                "name": trigger_task_name,
                "type": "triggerDagRun",
                "trigger_dag_id": dag_id,
                "wait_for_completion": True,
                "reset_dag_run": False,
                "depends_on": [],
            }
        )
        tasks.append(
            {
                "name": wait_task_name,
                "type": "externalTaskSensor",
                "external_dag_id": dag_id,
                "external_task_id": external_task_id,
                "depends_on": [trigger_task_name],
                "allowed_states": ["success"],
                "failed_states": ["failed"],
                "timeout": 7200,
                "poke_interval": 60,
                "deferrable": False,
                "mode": "reschedule",
            }
        )
        wait_task_names.append(wait_task_name)

    validate_task_name = normalize_task_name(f"{task_prefix}_validate_table")
    refresh_task_name = normalize_task_name(f"{task_prefix}_refresh_sheet")

    tasks.append(
        {
            "name": validate_task_name,
            "type": "sql",
            "query": f"SELECT * FROM public.{table.strip()} LIMIT 1;",
            "table": table.strip(),
            "depends_on": wait_task_names,
        }
    )
    tasks.append(
        {
            "name": refresh_task_name,
            "type": "bash",
            "command": (
                'ROOT_DIR="$(dirname "$(dirname "{{ task.dag.fileloc }}")")" && '
                'python "$ROOT_DIR/streamlit/plugin/create_yaml/refresh_report_sheet.py" '
                '--spreadsheet-id "{{ params.spreadsheet_id }}" '
                '--spreadsheet-title "{{ params.spreadsheet_title }}" '
                '--sheet-name "{{ params.sheet_name }}" '
                '--table-name "{{ params.table_name }}"'
            ),
            "params": {
                "spreadsheet_id": spreadsheet_id.strip(),
                "spreadsheet_title": spreadsheet_title.strip(),
                "sheet_name": sheet_name.strip(),
                "table_name": table.strip(),
            },
            "depends_on": [validate_task_name],
        }
    )

    payload = {
        "schedule": schedule.strip(),
        "project": project.strip(),
        "description": description or f"Generated scheduler DAG for template {template_name}.",
        "template_name": template_name.strip(),
        "table": table.strip(),
        "spreadsheet_id": spreadsheet_id.strip(),
        "spreadsheet_title": spreadsheet_title.strip(),
        "sheet_name": sheet_name.strip(),
        "dags_refresh": dags_refresh_items,
        "tasks": tasks,
    }

    with open(output_path, "w", encoding="utf-8") as yaml_file:
        yaml.safe_dump(payload, yaml_file, sort_keys=False, allow_unicode=False)

    return output_path
