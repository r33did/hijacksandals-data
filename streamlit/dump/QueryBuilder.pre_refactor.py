import pandas as pd
from typing import List, Dict, Optional, Any
from sqlalchemy import create_engine
import psycopg2
import yaml
import os 
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()
user = os.getenv("USER")
password = os.getenv("PASSWORD")
host = os.getenv("HOST")
port = os.getenv("PORT")
database = os.getenv("DATABASE")

BASE_DIR = Path(__file__).resolve() # Resolve Where you are 
ROOT_DIR = BASE_DIR.parents[3] # Angka diambil dari urutan folder/file

TEMPLATE_PATH = os.path.join(ROOT_DIR, os.getenv("TEMPLATE_QUERY"))

class Engine:
    _dataset = "testing"
    def __init__(self):
        self.engine = create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}", pool_pre_ping=True
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
        """
        Fetches all user-defined table names within the connected PostgreSQL database.
        Assumes `conn` is a valid psycopg2 connection or SQLAlchemy engine.
        """
        query = f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{self._dataset}'
            ORDER BY table_name;
        """
        try:
            df = pd.read_sql(query, self.connect())
            return df['table_name'].tolist()
        except Exception as e:
            print(f"Error fetching tables: {e}")
            return [] 
        finally:
            self.close()


    def get_table_columns(self, table_name: str) -> Dict[str, str]:
        """
        Fetches all columns and their mapped data types for a given table in PostgreSQL.
        """
        
        query = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{self._dataset}' AND table_name = '{table_name}';
        """
        try:
            df = pd.read_sql(query, self.connect())
            # Returns a dict of {column_name: data_type}
            return dict(zip(df['column_name'], df['data_type']))
        except Exception as e:
            print(f"Error fetching columns for table {table_name}: {e}")
            return {}
        finally:
            self.close()

    def build_dynamic_query(
        self, table_name: str, 
        date_column: Optional[str] = None, 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None,
        conditions: List[Dict[str, Any]] = None
    ) -> tuple[str, list]:
        """
        Builds a dynamic SELECT query and returns the query string along with the parameters tuple.
        
        `conditions` format example:
        [
            {"column": "status", "operator": "=", "value": "active", "type": "character varying"},
            {"column": "price", "operator": ">", "value": 100, "type": "integer"},
            {"column": "name", "operator": "ILIKE", "value": "%test%", "type": "text"}
        ]
        
        Supported operators: =, !=, <, >, <=, >=, LIKE, ILIKE, IN
        """
        
        if conditions is None:
            conditions = []
            
        query = f"SELECT * FROM {self._dataset}.{table_name}"
        where_clauses = []
        params = []
        
        # 1. Handle Date Filtering natively
        if date_column and start_date and end_date:
            where_clauses.append(f"{date_column} >= %s AND {date_column} <= %s")
            params.extend([start_date, end_date])
        elif date_column and start_date:
            where_clauses.append(f"{date_column} >= %s")
            params.append(start_date)
        elif date_column and end_date:
            where_clauses.append(f"{date_column} <= %s")
            params.append(end_date)
            
        # 2. Handle dynamically added conditions based on UI
        for idx, cond in enumerate(conditions):
            col = cond.get('column')
            op = cond.get('operator', '=').upper()
            val = cond.get('value')
            col_type = cond.get('type', 'text').lower()
            
            # Security/Validation: Minimal prevention against bad operators
            valid_operators = ['=', '!=', '<', '>', '<=', '>=', 'LIKE', 'ILIKE', 'IN']
            if op not in valid_operators:
                continue
                
            if op == 'IN' and isinstance(val, (list, tuple)):
                # Handling IN clause like: column IN (%s, %s, %s)
                placeholders = ', '.join(['%s'] * len(val))
                where_clauses.append(f"{col} {op} ({placeholders})")
                params.extend(val)
            elif 'char' in col_type or 'text' in col_type:
                # String-like operations
                if op in ['LIKE', 'ILIKE']:
                    where_clauses.append(f"{col} {op} %s")
                    params.append(f"%{val}%") # Wrap in % automatically if desired, or assume user provides it
                else:
                    where_clauses.append(f"{col} {op} %s")
                    params.append(val)
            elif 'int' in col_type or 'numeric' in col_type or 'float' in col_type or 'real' in col_type:
                # Numeric operations
                where_clauses.append(f"{col} {op} %s")
                params.append(val)
            else:
                # Fallback for booleans, dates, etc.
                where_clauses.append(f"{col} {op} %s")
                params.append(val)

        # Combine where clauses
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
            
        return query, params

    def execute_query(self, query: str, params: tuple):
        """
        Utility to execute the dynamically built parameterized query securely via Pandas.
        """
        try:
            # Pandas read_sql supports parameterized queries depending on the driver
            # For psycopg2 via SQLAlchemy/raw strings, `params` can be passed as a tuple/list.
            df = pd.read_sql(query, self.connect(), params=params)
            return df
        except Exception as e:
            print(f"Failed to execute query: {e}")
            return pd.DataFrame()
        finally:
            self.close()


class ReadTemplate():
    def __init__(self, path = TEMPLATE_PATH):
        self.path = path

        with open(self.path, "r") as f:
            self.template_query = yaml.safe_load(f) or {}

    # =========================
    # GET
    # =========================
    def get_query(self, template_name: str):
        return self.template_query.get(template_name)

    # =========================
    # ADD / UPDATE
    # =========================
    def upsert_query(self, template_name: str, query: str):
        self.template_query[template_name] = query
        self._save()

    # =========================
    # DELETE
    # =========================
    def delete_query(self, template_name: str):
        if template_name in self.template_query:
            del self.template_query[template_name]
            self._save()
            return True
        return False

    # =========================
    # LIST
    # =========================
    def list_templates(self):
        return list(self.template_query.keys())

    # =========================
    # SAVE (PRIVATE)
    # =========================
    def _save(self):
        with open(self.path, "w") as f:
            yaml.safe_dump(self.template_query, f, sort_keys=False)
