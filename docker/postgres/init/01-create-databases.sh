#!/bin/sh
set -eu

create_db_if_missing() {
  db_name="$1"

  if [ -z "$db_name" ]; then
    return
  fi

  if psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -tAc "SELECT 1 FROM pg_database WHERE datname='${db_name}'" | grep -q 1; then
    echo "Database '${db_name}' already exists"
  else
    echo "Creating database '${db_name}'"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "CREATE DATABASE \"${db_name}\""
  fi
}

create_db_if_missing "${ETL_DB_NAME:-}"
create_db_if_missing "${GITEA_DB_NAME:-}"
