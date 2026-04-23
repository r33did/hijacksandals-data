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

HOURLY_SCHEDULE = "0 0 * * *"


def get_dimension_loader_keys():
    if hasattr(etl, "DIMENSION_DAG_LOADER_KEYS"):
        return etl.DIMENSION_DAG_LOADER_KEYS

    return [
        loader_key
        for loader_key in etl.LOADER_CONFIGS
        if not loader_key.startswith("fact_")
    ]


def run_dimension_loader(loader_key, **context):
    del context
    etl.run_loader(loader_key)


for loader_key in get_dimension_loader_keys():
    loader_config = etl.LOADER_CONFIGS[loader_key]
    dag_id = f"dealpos_{loader_key}_hourly"

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f"Hourly DealPOS dimension loader for {loader_config['name']}",
        start_date=datetime(2026, 4, 13),
        schedule=HOURLY_SCHEDULE,
        catchup=False,
        max_active_runs=1,
        tags=["dealpos", "dimension", loader_key],
    )

    with dag:
        PythonOperator(
            task_id=f"load_{loader_key}",
            python_callable=run_dimension_loader,
            op_kwargs={"loader_key": loader_key},
        )

    globals()[dag_id] = dag
