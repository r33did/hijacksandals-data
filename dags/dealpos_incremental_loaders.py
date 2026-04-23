from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import etl


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

HOURLY_SCHEDULE = "0 * * * *"
FACT_LOOKBACK_DAYS = 1


def get_fact_loader_keys():
    if hasattr(etl, "FACT_DAG_LOADER_KEYS"):
        return etl.FACT_DAG_LOADER_KEYS

    return [
        loader_key
        for loader_key, loader_config in etl.LOADER_CONFIGS.items()
        if loader_key.startswith("fact_")
        or loader_config.get("supports_incremental_window", False)
    ]


def run_incremental_loader(loader_key, **context):
    loader_config = etl.LOADER_CONFIGS[loader_key]
    start_date = None
    end_date = None

    if loader_config["supports_incremental_window"]:
        interval_start = context["data_interval_start"] - timedelta(days=FACT_LOOKBACK_DAYS)
        interval_end = context["data_interval_end"]
        start_date = interval_start.strftime("%Y-%m-%d")
        end_date = interval_end.strftime("%Y-%m-%d")

    etl.run_loader(loader_key, start_date=start_date, end_date=end_date)

for loader_key in get_fact_loader_keys():
    loader_config = etl.LOADER_CONFIGS[loader_key]
    dag_id = f"dealpos_{loader_key}_hourly"

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f"Hourly DealPOS loader for {loader_config['name']}",
        start_date=datetime(2026, 4, 13),
        schedule=HOURLY_SCHEDULE,
        catchup=False,
        max_active_runs=1,
        tags=["dealpos", "incremental", loader_key],
    )

    with dag:
        PythonOperator(
            task_id=f"load_{loader_key}",
            python_callable=run_incremental_loader,
            op_kwargs={"loader_key": loader_key},
        )

    globals()[dag_id] = dag
