import psycopg2
from dotenv import load_dotenv
from . import helper
from .helper import *

load_dotenv()
PLUGIN_NAME = "dealpos"
DEFAULT_DAG_PREFIX = "dealpos"

LOADER_CONFIGS = {
    "outlets": {
        "name": "Outlets",
        "callable": load_outlets,
        "supports_incremental_window": False,
        "source_mode": "full_resync",
        "tags": ["dealpos", "dimension"],
    },
    "categories": {
        "name": "Categories",
        "callable": load_categories,
        "supports_incremental_window": False,
        "source_mode": "full_resync",
        "tags": ["dealpos", "dimension"],
    },
    "customers": {
        "name": "Customers",
        "callable": load_customers,
        "supports_incremental_window": False,
        "source_mode": "full_resync",
        "tags": ["dealpos", "dimension"],
    },
    "products": {
        "name": "Products and Variants",
        "callable": load_products,
        "supports_incremental_window": False,
        "source_mode": "full_resync",
        "tags": ["dealpos", "dimension"],
    },
    "variant_data": {
        "name": "Variant Data",
        "callable": load_variant_data,
        "supports_incremental_window": False,
        "source_mode": "full_resync",
        "tags": ["dealpos", "dimension"],
    },
    "suppliers": {
        "name": "Suppliers",
        "callable": load_suppliers,
        "supports_incremental_window": False,
        "source_mode": "full_resync",
        "tags": ["dealpos", "dimension"],
    },
    "payment_methods": {
        "name": "Payment Methods",
        "callable": load_payment_methods,
        "supports_incremental_window": False,
        "source_mode": "full_resync",
        "tags": ["dealpos", "dimension"],
    },
    "taxes": {
        "name": "Taxes",
        "callable": load_taxes,
        "supports_incremental_window": False,
        "source_mode": "full_resync",
        "tags": ["dealpos", "dimension"],
    },
    "users": {
        "name": "Users",
        "callable": load_users,
        "supports_incremental_window": False,
        "source_mode": "full_resync",
        "tags": ["dealpos", "dimension"],
    },
    "fact_invoice": {
        "name": "Fact Invoice",
        "callable": load_fact_invoice,
        "supports_incremental_window": True,
        "source_mode": "rolling_window",
        "tags": ["dealpos", "incremental"],
    },
    "fact_invoice_line": {
        "name": "Fact Invoice Line",
        "callable": load_fact_invoice_line,
        "supports_incremental_window": True,
        "source_mode": "rolling_window",
        "tags": ["dealpos", "incremental"],
    },
    "fact_invoice_return": {
        "name": "Fact Invoice Return",
        "callable": load_fact_invoice_return,
        "supports_incremental_window": True,
        "source_mode": "rolling_window",
        "tags": ["dealpos", "incremental"],
    },
    "fact_inventory": {
        "name": "Fact Inventory",
        "callable": load_fact_inventory,
        "supports_incremental_window": False,
        "source_mode": "full_resync",
        "tags": ["dealpos", "incremental"],
    },
}

DIMENSION_DAG_LOADER_KEYS = [
    "outlets",
    "categories",
    "customers",
    "products",
    "variant_data",
    "suppliers",
    "payment_methods",
    "taxes",
    "users",
]

FACT_DAG_LOADER_KEYS = [
    "fact_invoice",
    "fact_invoice_line",
    "fact_invoice_return",
    "fact_inventory",
]

HOURLY_DAG_LOADER_KEYS = DIMENSION_DAG_LOADER_KEYS + FACT_DAG_LOADER_KEYS


def run_loader(loader_key, start_date=None, end_date=None):
    if loader_key not in LOADER_CONFIGS:
        raise ValueError(f"Unknown loader key: {loader_key}")

    loader_config = LOADER_CONFIGS[loader_key]
    loader_name = loader_config["name"]
    loader = loader_config["callable"]
    source_mode = loader_config["source_mode"]

    log(f"Getting token for loader: {loader_name}")
    helper.TOKEN = get_token()

    log(f"Connecting to PostgreSQL for loader: {loader_name}")
    connection = psycopg2.connect(**DB_CONFIG)
    cursor = connection.cursor()

    try:
        ensure_sync_state_table(cursor)
        mark_loader_started(cursor, loader_key, loader_name, source_mode)
        connection.commit()

        if start_date or end_date:
            resolved_start, resolved_end = resolve_date_window(start_date, end_date)
            log(f"Running {loader_name} with window {resolved_start} to {resolved_end}.")
            loader(cursor, start_date=resolved_start, end_date=resolved_end)
        else:
            log(f"Running {loader_name} without a date window.")
            loader(cursor)

        mark_loader_finished(cursor, loader_key, "success")
        connection.commit()
        log(f"{loader_name} finished successfully.")
    except Exception as exc:
        connection.rollback()
        error_connection = psycopg2.connect(**DB_CONFIG)
        error_cursor = error_connection.cursor()
        try:
            ensure_sync_state_table(error_cursor)
            mark_loader_finished(error_cursor, loader_key, "failed", str(exc)[:2000])
            error_connection.commit()
        finally:
            error_cursor.close()
            error_connection.close()
        raise
    finally:
        cursor.close()
        connection.close()
