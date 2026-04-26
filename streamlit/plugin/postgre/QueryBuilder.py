import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine, engine

BASE_DIR = Path(__file__).resolve()
ROOT_DIR = BASE_DIR.parents[3]
STREAMLIT_DIR = ROOT_DIR / "streamlit"
ENV_PATH = ROOT_DIR / ".env"

load_dotenv(ENV_PATH)

def get_env_value(*keys: str, default: str | None = None) -> str | None:
    for key in keys:
        value = os.getenv(key)
        if value is not None and value != "":
            return value.strip().strip("'\"")
    return default


def resolve_env_path(env_key: str, default: str) -> Path:
    raw_value = get_env_value(env_key, default=default)
    normalized = Path(str(raw_value).replace("\\", "/"))
    candidate_paths: list[Path] = []

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
        if candidate.exists():
            return candidate

    return candidate_paths[-1]


user = get_env_value("ETL_DB_USER", default=None)
password = get_env_value("ETL_DB_PASSWORD", default=None)
host = get_env_value( "POSTGRES_HOST", default=None)
port = get_env_value("ETL_DB_PORT",  default=None)
database = get_env_value("ETL_DB_NAME", default="postgres")

TEMPLATE_PATH = resolve_env_path("TEMPLATE_QUERY", "streamlit/plugin/postgre/templatequery.yaml")


class Engine:
    _dataset = "public"

    def __init__(self):
        self.engine = create_engine(
            engine.URL.create(
                drivername="postgresql+psycopg2",
                username=user,
                password=password,
                host=host,
                port=port,
                database=database,
            )
            ,
            pool_pre_ping=True,
        )

    def connect(self):
        return self.engine.raw_connection()

    def ping(self):
        try:
            self.engine.connect().execute("SELECT 1")
            return True
        except Exception as e:
            print(f"Error pinging database: {e}")
            return False

    def close(self):
        self.engine.dispose()

    def get_all_tables(self) -> List[str]:
        query = f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{self._dataset}'
            ORDER BY table_name;
        """
        try:
            df = pd.read_sql(query, self.connect())
            return df["table_name"].tolist()
        except Exception as e:
            print(f"Error fetching tables: {e}")
            return []
        finally:
            self.close()

    def get_all_views(self) -> List[str]:
        query = f"""
            SELECT table_name
            FROM information_schema.views
            WHERE table_schema = '{self._dataset}'
            ORDER BY table_name;
        """
        try:
            df = pd.read_sql(query, self.connect())
            return df["table_name"].tolist()
        except Exception as e:
            print(f"Error fetching views: {e}")
            return []
        finally:
            self.close()

    def get_table_columns(self, table_name: str) -> Dict[str, str]:
        query = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{self._dataset}' AND table_name = '{table_name}';
        """
        try:
            df = pd.read_sql(query, self.connect())
            return dict(zip(df["column_name"], df["data_type"]))
        except Exception as e:
            print(f"Error fetching columns for table {table_name}: {e}")
            return {}
        finally:
            self.close()

    def build_dynamic_query(
        self,
        table_name: str,
        date_column: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        conditions: List[Dict[str, Any]] = None,
    ) -> tuple[str, list]:
        if conditions is None:
            conditions = []

        query = f"SELECT * FROM {self._dataset}.{table_name}"
        where_clauses = []
        params = []

        if date_column and start_date and end_date:
            where_clauses.append(f"{date_column} >= %s AND {date_column} <= %s")
            params.extend([start_date, end_date])
        elif date_column and start_date:
            where_clauses.append(f"{date_column} >= %s")
            params.append(start_date)
        elif date_column and end_date:
            where_clauses.append(f"{date_column} <= %s")
            params.append(end_date)

        for cond in conditions:
            col = cond.get("column")
            op = cond.get("operator", "=").upper()
            val = cond.get("value")
            col_type = cond.get("type", "text").lower()

            valid_operators = ["=", "!=", "<", ">", "<=", ">=", "LIKE", "ILIKE", "IN"]
            if op not in valid_operators:
                continue

            if op == "IN" and isinstance(val, (list, tuple)):
                placeholders = ", ".join(["%s"] * len(val))
                where_clauses.append(f"{col} {op} ({placeholders})")
                params.extend(val)
            elif "char" in col_type or "text" in col_type:
                if op in ["LIKE", "ILIKE"]:
                    where_clauses.append(f"{col} {op} %s")
                    params.append(f"%{val}%")
                else:
                    where_clauses.append(f"{col} {op} %s")
                    params.append(val)
            else:
                where_clauses.append(f"{col} {op} %s")
                params.append(val)

        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        return query, params

    def execute_query(self, query: str, params: tuple):
        try:
            df = pd.read_sql(query, self.connect(), params=params)
            return df
        except Exception as e:
            print(f"Failed to execute query: {e}")
            return pd.DataFrame()
        finally:
            self.close()


class ReadTemplate:
    def __init__(self, path=TEMPLATE_PATH):
        self.path = Path(path)
        self.template_query: dict[str, Any] = {}
        self.reload()

    def reload(self):
        with open(self.path, "r", encoding="utf-8") as template_file:
            self.template_query = yaml.safe_load(template_file) or {}
        return self.template_query

    def _get_template_entry(self, template_name: str) -> dict[str, Any]:
        self.reload()
        template_entry = self.template_query.get(template_name, {})
        if isinstance(template_entry, dict):
            return template_entry
        if template_entry:
            return {"query": template_entry}
        return {}

    def get_query(self, template_name: str):
        template_entry = self._get_template_entry(template_name)
        stored_query = template_entry.get("query")
        if stored_query:
            return str(stored_query).strip()

        table_name = template_entry.get("table")
        if table_name:
            return self.build_table_query(str(table_name))
        return None

    def get_table(self, template_name: str) -> str | None:
        template_entry = self._get_template_entry(template_name)
        table_name = template_entry.get("table")
        return str(table_name).strip() if table_name else None

    def _derive_loader_key_from_dag_id(self, dag_id: str) -> str:
        normalized = str(dag_id).strip()
        if normalized.endswith("_hourly"):
            normalized = normalized[: -len("_hourly")]
        if "_" in normalized:
            normalized = normalized.split("_", maxsplit=1)[1]
        return normalized

    def normalize_dags_refresh_items(self, dags_refresh: Any) -> list[dict[str, str]]:
        if dags_refresh is None:
            return []

        if isinstance(dags_refresh, (str, dict)):
            dags_refresh = [dags_refresh]

        normalized_items: list[dict[str, str]] = []
        for raw_item in dags_refresh:
            if isinstance(raw_item, str):
                dag_id = raw_item.strip()
                if not dag_id:
                    continue
                loader_key = self._derive_loader_key_from_dag_id(dag_id)
                normalized_items.append(
                    {
                        "dag_id": dag_id,
                        "loader_key": loader_key,
                        "external_task_id": f"load_{loader_key}",
                    }
                )
                continue

            if not isinstance(raw_item, dict):
                continue

            dag_id = str(raw_item.get("dag_id", "")).strip()
            if not dag_id:
                continue

            loader_key = str(raw_item.get("loader_key", "")).strip() or self._derive_loader_key_from_dag_id(dag_id)
            external_task_id = str(raw_item.get("external_task_id", "")).strip() or f"load_{loader_key}"

            normalized_items.append(
                {
                    "dag_id": dag_id,
                    "loader_key": loader_key,
                    "external_task_id": external_task_id,
                }
            )

        return normalized_items

    def get_dags_refresh_items(self, template_name: str) -> list[dict[str, str]]:
        template_entry = self._get_template_entry(template_name)
        return self.normalize_dags_refresh_items(template_entry.get("dags_refresh", []))

    def get_dags_refresh(self, template_name: str) -> list[str]:
        return [item["dag_id"] for item in self.get_dags_refresh_items(template_name)]

    def get_description(self, template_name: str) -> str:
        template_entry = self._get_template_entry(template_name)
        description = template_entry.get("description")
        if description:
            return str(description)
        return self._infer_description(self.get_query(template_name), self.get_table(template_name))

    def get_template_context(self) -> list[dict[str, str]]:
        return [
            {
                "template_name": template_name,
                "description": self.get_description(template_name),
                "table": self.get_table(template_name) or "-",
                "dags_refresh": ", ".join(self.get_dags_refresh(template_name)) or "-",
            }
            for template_name in self.list_templates()
        ]

    def upsert_template(
        self,
        template_name: str,
        table: str,
        description: Optional[str] = None,
        dags_refresh: Optional[list[str]] = None,
    ):
        self.reload()
        existing_template = self._get_template_entry(template_name)
        cleaned_dags_refresh = self.normalize_dags_refresh_items(
            dags_refresh if dags_refresh is not None else self.get_dags_refresh_items(template_name)
        )

        self.template_query[template_name] = {
            "description": description or existing_template.get("description") or self._infer_description(None, table),
            "table": table,
            "dags_refresh": cleaned_dags_refresh,
        }
        self._save()

    def upsert_query(self, template_name: str, query: str, description: Optional[str] = None):
        self.reload()
        existing_template = self._get_template_entry(template_name)
        self.template_query[template_name] = {
            "description": description or existing_template.get("description") or "Template based on saved SQL query.",
            "query": query,
            "table": existing_template.get("table"),
            "dags_refresh": existing_template.get("dags_refresh", []),
        }
        self._save()

    def delete_query(self, template_name: str):
        self.reload()
        if template_name in self.template_query:
            del self.template_query[template_name]
            self._save()
            return True
        return False

    def list_templates(self):
        self.reload()
        valid_templates: list[str] = []
        for template_name in self.template_query.keys():
            if template_name.startswith("{"):
                continue
            template_entry = self._get_template_entry(template_name)
            if template_entry.get("query") or template_entry.get("table"):
                valid_templates.append(template_name)
        return valid_templates

    def list_available_views(self) -> list[str]:
        return Engine().get_all_views()

    def _save(self):
        with open(self.path, "w", encoding="utf-8") as template_file:
            yaml.safe_dump(self.template_query, template_file, sort_keys=False)

    def build_table_query(self, table_name: str, limit: int | None = None) -> str:
        query = f"SELECT * FROM {Engine._dataset}.{table_name}"
        if limit is not None:
            query = f"{query}\nLIMIT {limit}"
        return f"{query};"

    def _infer_description(self, query: Optional[str], table_name: Optional[str] = None) -> str:
        if table_name:
            return f"Dataset extract from {table_name}."
        if not query:
            return "No description available."

        normalized_query = " ".join(str(query).split())
        source_match = re.search(r"\bfrom\s+([^\s;]+)", normalized_query, flags=re.IGNORECASE)
        source_name = source_match.group(1).replace('"', "") if source_match else None

        prefix = "Aggregated report" if "group by" in normalized_query.lower() else "Dataset extract"
        if source_name:
            return f"{prefix} from {source_name}."
        return f"{prefix} based on the saved SQL template."
