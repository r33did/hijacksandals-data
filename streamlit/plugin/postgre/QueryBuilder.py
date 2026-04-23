import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine

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


user = get_env_value("ETL_DB_USER", "POSTGRES_USER", "USER", default="airflow")
password = get_env_value("ETL_DB_PASSWORD", "POSTGRES_PASSWORD", "PASSWORD", default="airflow")
host = get_env_value("ETL_DB_HOST", "POSTGRES_HOST", "HOST", default="localhost")
port = get_env_value("ETL_DB_PORT", "POSTGRES_PORT", "PORT", default="5432")
database = get_env_value("ETL_DB_NAME", "POSTGRES_DB", "DATABASE", default="airflow")

TEMPLATE_PATH = resolve_env_path("TEMPLATE_QUERY", "streamlit/plugin/postgre/templatequery.yaml")


class Engine:
    _dataset = "testing"

    def __init__(self):
        self.engine = create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}",
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

        with open(self.path, "r", encoding="utf-8") as template_file:
            self.template_query = yaml.safe_load(template_file) or {}

    def get_query(self, template_name: str):
        template_entry = self.template_query.get(template_name)
        if isinstance(template_entry, dict):
            return template_entry.get("query")
        return template_entry

    def get_description(self, template_name: str) -> str:
        template_entry = self.template_query.get(template_name)
        if isinstance(template_entry, dict):
            description = template_entry.get("description")
            if description:
                return str(description)
        return self._infer_description(self.get_query(template_name))

    def get_template_context(self) -> list[dict[str, str]]:
        return [
            {
                "template_name": template_name,
                "description": self.get_description(template_name),
            }
            for template_name in self.list_templates()
        ]

    def upsert_query(self, template_name: str, query: str, description: Optional[str] = None):
        existing_template = self.template_query.get(template_name)
        if description is None and isinstance(existing_template, dict):
            description = existing_template.get("description")

        if description:
            self.template_query[template_name] = {
                "description": description,
                "query": query,
            }
        else:
            self.template_query[template_name] = query
        self._save()

    def delete_query(self, template_name: str):
        if template_name in self.template_query:
            del self.template_query[template_name]
            self._save()
            return True
        return False

    def list_templates(self):
        valid_templates: list[str] = []
        for template_name in self.template_query.keys():
            if template_name.startswith("{"):
                continue
            if self.get_query(template_name):
                valid_templates.append(template_name)
        return valid_templates

    def _save(self):
        with open(self.path, "w", encoding="utf-8") as template_file:
            yaml.safe_dump(self.template_query, template_file, sort_keys=False)

    def _infer_description(self, query: Optional[str]) -> str:
        if not query:
            return "No description available."

        normalized_query = " ".join(str(query).split())
        source_match = re.search(r"\bfrom\s+([^\s;]+)", normalized_query, flags=re.IGNORECASE)
        source_name = source_match.group(1).replace('"', "") if source_match else None

        prefix = "Aggregated report" if "group by" in normalized_query.lower() else "Dataset extract"
        if source_name:
            return f"{prefix} from {source_name}."
        return f"{prefix} based on the saved SQL template."
