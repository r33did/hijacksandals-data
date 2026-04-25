"""
Hijack Sandals ETL loader.

This script supports both:
- initial/manual loads by running the file directly
- hourly Airflow-triggered loader runs through `run_loader`
"""

import os
import time
from datetime import datetime

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

load_dotenv()


BASE_URL = "https://hijacksandals.dealpos.net/api/v3"
CLIENT_ID = "019c9a25-05c4-7a38-b04a-dd1efe4c08ef"
CLIENT_SECRET = "019c9a26-51bf-7232-ae0a-4c702ba345e9"

DB_CONFIG = {
    "host": os.getenv("ETL_DB_HOST"),
    "port": int(os.getenv("ETL_DB_PORT")),
    "dbname": os.getenv("ETL_DB_NAME"),
    "user": os.getenv("ETL_DB_USER"),
    "password": os.getenv("ETL_DB_PASSWORD"),
}

DEFAULT_START_DATE = "2026-03-01"
DEFAULT_END_DATE = datetime.today().strftime("%Y-%m-%d")
PAGE_SIZE = 500
DEFAULT_BATCH_SIZE = 1000
TOKEN = None
SYNC_STATE_TABLE = "etl_sync_state"


def log(message):
    print(message)


def get_token():
    response = requests.post(
        f"{BASE_URL}/Token/OAuth2",
        json={"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET},
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()

    payload = response.json()
    token = f"{payload['token_type']} {payload['access_token']}"
    log(f"Token acquired: {token[:30]}...")
    return token


def api_get(endpoint, params=None, retries=3, ignore_statuses=None):
    ignore_statuses = set(ignore_statuses or [])

    for _ in range(retries):
        response = requests.get(
            f"{BASE_URL}/{endpoint}",
            headers={"Authorization": TOKEN, "Accept": "*/*"},
            params=params,
        )

        if response.status_code == 429:
            try:
                wait_seconds = (
                    int("".join(filter(str.isdigit, response.json().get("Message", ""))))
                    or 180
                )
            except Exception:
                wait_seconds = 180

            log(f"Rate limited. Waiting {wait_seconds}s before retrying.")
            time.sleep(wait_seconds + 2)
            continue

        if response.status_code in ignore_statuses:
            log(f"Skipping {endpoint}: {response.status_code} {response.url}")
            return []

        if not response.ok:
            log(
                f"Request failed: {response.status_code} {response.url} - "
                f"{response.text[:200]}"
            )
            response.raise_for_status()

        return response.json()

    raise Exception(f"Request failed after {retries} retries: {endpoint}")


def api_post(endpoint, body, retries=3):
    for _ in range(retries):
        response = requests.post(
            f"{BASE_URL}/{endpoint}",
            headers={
                "Authorization": TOKEN,
                "Accept": "*/*",
                "Content-Type": "application/json",
            },
            json=body,
        )

        if response.status_code == 429:
            try:
                wait_seconds = (
                    int("".join(filter(str.isdigit, response.json().get("Message", ""))))
                    or 180
                )
            except Exception:
                wait_seconds = 180

            log(f"Rate limited. Waiting {wait_seconds}s before retrying.")
            time.sleep(wait_seconds + 2)
            continue

        if not response.ok:
            log(f"Request failed: {response.status_code} {endpoint} - {response.text[:200]}")
            response.raise_for_status()

        return response.json()

    raise Exception(f"Request failed after {retries} retries: {endpoint}")


def safe_ts(value):
    """Return None for empty or unsupported timestamps."""
    if not value:
        return None

    try:
        parsed = datetime.fromisoformat(value)
        if parsed.year < 1970:
            return None
        return value
    except Exception:
        return None


def to_list(data):
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    return []


def upsert(cursor, table, rows, pk):
    if not rows:
        log(f"No data to load for {table}.")
        return

    columns = list(rows[0].keys())
    placeholders = ", ".join(["%s"] * len(columns))
    column_names = ", ".join(columns)
    updates = ", ".join(f"{column}=EXCLUDED.{column}" for column in columns if column != pk)

    sql = f"""
        INSERT INTO {table} ({column_names})
        VALUES ({placeholders})
        ON CONFLICT ({pk}) DO UPDATE SET {updates}
    """
    values = [tuple(row.get(column) for column in columns) for row in rows]
    psycopg2.extras.execute_batch(cursor, sql, values)
    log(f"{table}: {len(rows)} rows upserted.")


def ensure_sync_state_table(cursor):
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SYNC_STATE_TABLE} (
            loader_key TEXT PRIMARY KEY,
            loader_name TEXT NOT NULL,
            last_status TEXT,
            last_started_at TIMESTAMP,
            last_finished_at TIMESTAMP,
            last_success_at TIMESTAMP,
            last_error TEXT,
            source_mode TEXT NOT NULL DEFAULT 'full_resync',
            updated_at TIMESTAMP NOT NULL DEFAULT NOW()
        )
        """
    )


def mark_loader_started(cursor, loader_key, loader_name, source_mode):
    cursor.execute(
        f"""
        INSERT INTO {SYNC_STATE_TABLE} (
            loader_key,
            loader_name,
            last_status,
            last_started_at,
            last_finished_at,
            last_success_at,
            last_error,
            source_mode,
            updated_at
        )
        VALUES (%s, %s, 'running', NOW(), NULL, NULL, NULL, %s, NOW())
        ON CONFLICT (loader_key) DO UPDATE SET
            loader_name = EXCLUDED.loader_name,
            last_status = 'running',
            last_started_at = NOW(),
            last_finished_at = NULL,
            last_error = NULL,
            source_mode = EXCLUDED.source_mode,
            updated_at = NOW()
        """,
        (loader_key, loader_name, source_mode),
    )


def mark_loader_finished(cursor, loader_key, status, error_message=None):
    if status == "success":
        cursor.execute(
            f"""
            UPDATE {SYNC_STATE_TABLE}
            SET
                last_status = 'success',
                last_finished_at = NOW(),
                last_success_at = NOW(),
                last_error = NULL,
                updated_at = NOW()
            WHERE loader_key = %s
            """,
            (loader_key,),
        )
    else:
        cursor.execute(
            f"""
            UPDATE {SYNC_STATE_TABLE}
            SET
                last_status = 'failed',
                last_finished_at = NOW(),
                last_error = %s,
                updated_at = NOW()
            WHERE loader_key = %s
            """,
            (error_message, loader_key),
        )


def resolve_date_window(start_date=None, end_date=None):
    return start_date or DEFAULT_START_DATE, end_date or DEFAULT_END_DATE


def paginate_post(endpoint, extra_body=None, max_pages=None, start_date=None, end_date=None):
    """Read a paginated POST endpoint into a flat list."""
    window_start, window_end = resolve_date_window(start_date, end_date)
    all_rows = []
    page = 1

    while True:
        body = {
            "From": window_start,
            "To": window_end,
            "PageNumber": page,
            "PageSize": PAGE_SIZE,
        }
        if extra_body:
            body.update(extra_body)

        page_rows = to_list(api_post(endpoint, body))
        if not page_rows:
            break

        all_rows.extend(page_rows)
        log(f"Page {page}: {len(page_rows)} rows fetched (total: {len(all_rows)}).")

        if len(page_rows) < PAGE_SIZE:
            break

        if max_pages and page >= max_pages:
            log(f"Reached page limit ({max_pages}). Stopping early.")
            break

        page += 1
        time.sleep(1)

    return all_rows


def paginate_get(endpoint, page_size=DEFAULT_BATCH_SIZE, extra_params=None, data_key=None):
    """Read a paginated GET endpoint into a flat list."""
    all_rows = []
    page = 1

    while True:
        params = {"PageNumber": page, "PageSize": page_size}
        if extra_params:
            params.update(extra_params)

        response = api_get(endpoint, params)
        page_rows = response.get(data_key, []) if data_key and isinstance(response, dict) else to_list(response)
        if not page_rows:
            break

        all_rows.extend(page_rows)
        log(f"Page {page}: {len(page_rows)} rows fetched (total: {len(all_rows)}).")

        if len(page_rows) < page_size:
            break

        page += 1
        time.sleep(1)

    return all_rows


def get_latest_loaded_at(cursor, table_name):
    cursor.execute(f"SELECT MAX(_loaded_at) FROM {table_name}")
    return cursor.fetchone()[0]


def load_outlets(cursor, start_date=None, end_date=None):
    outlets = to_list(api_get("Outlet", {"Suspended": "false"}))
    log(f"Found {len(outlets)} outlets.")
    rows = []

    for index, outlet in enumerate(outlets, start=1):
        code = outlet.get("Code", "")
        name = outlet.get("Name", "")

        try:
            detail = to_list(api_get("Outlet/Detail", {"Code": code, "Name": name}))
            if not detail:
                log(f"Skipping {code}: detail response was empty.")
                continue

            item = detail[0]
            receipt_template = item.get("ReceiptTemplate") or {}
            rows.append(
                {
                    "outlet_id": str(item.get("ID", "")),
                    "code": item.get("Code"),
                    "name": item.get("Name"),
                    "email": item.get("Email"),
                    "sales_target": item.get("SalesTarget", 0),
                    "minimum_inventory": item.get("MinimumInventory", 0),
                    "maximum_inventory": item.get("maximumInventory", 0),
                    "order_display": item.get("OrderDisplayBroadcastMode"),
                    "is_suspended": item.get("Suspended", False),
                    "receipt_code": receipt_template.get("Code"),
                    "outlet_name": receipt_template.get("OutletName"),
                    "address": receipt_template.get("Address"),
                    "contact_info": receipt_template.get("ContactInfo"),
                    "_loaded_at": datetime.now(),
                }
            )
            log(f"Outlet {index}/{len(outlets)} loaded: {code}")
        except Exception as exc:
            log(f"Skipping outlet {code}: {exc}")

        time.sleep(1)

    upsert(cursor, "dim_outlet", rows, "outlet_id")


def load_categories(cursor, start_date=None, end_date=None):
    data = to_list(api_get("Category"))
    rows = []

    for item in data:
        rows.append(
            {
                "category_id": str(item.get("ID", "")),
                "name": item.get("Name"),
                "is_active": not item.get("Discontinued", False),
                "_loaded_at": datetime.now(),
            }
        )

    upsert(cursor, "dim_category", rows, "category_id")


def load_customers(cursor, start_date=None, end_date=None):
    data = paginate_get("Customer", page_size=1000)
    rows = []

    for item in data:
        rows.append(
            {
                "customer_id": str(item.get("ID", "")),
                "code": item.get("Code"),
                "name": item.get("Name"),
                "first_name": item.get("FirstName"),
                "last_name": item.get("LastName"),
                "email": item.get("Email"),
                "mobile": item.get("Mobile"),
                "phone": item.get("Phone"),
                "birth_date": item.get("BirthDate"),
                "join_date": item.get("JoinDate"),
                "expired_date": item.get("ExpiredDate"),
                "national_id_number": item.get("NationalIDNumber"),
                "nationality_id": item.get("NationalityID"),
                "state_id": item.get("StateID"),
                "_loaded_at": datetime.now(),
            }
        )

    upsert(cursor, "dim_customer", rows, "customer_id")


def load_products(cursor, start_date=None, end_date=None):
    products = paginate_get("Product", page_size=1000, data_key="DataArray")
    log(f"Found {len(products)} products.")

    product_rows = []
    variant_rows = []

    for item in products:
        product_id = str(item.get("ID", ""))
        product_rows.append(
            {
                "product_id": product_id,
                "code": item.get("Code"),
                "name": item.get("Name"),
                "category": item.get("Category"),
                "released": item.get("Released"),
                "thumbnail_url": item.get("ThumbnailUrl"),
                "image_url": item.get("ImageUrl"),
                "is_active": not item.get("Discontinued", False),
                "created_at": item.get("Created"),
                "_loaded_at": datetime.now(),
            }
        )

        for variant in item.get("Variants") or []:
            variant_rows.append(
                {
                    "variant_id": str(variant.get("ID", "")),
                    "product_id": product_id,
                    "code": variant.get("Code"),
                    "model": variant.get("Model"),
                    "unit_price": variant.get("UnitPrice"),
                    "discount": variant.get("Discount"),
                    "weight": variant.get("Weight"),
                    "type_id": variant.get("TypeID"),
                    "is_active": not variant.get("Discontinued", False),
                    "_loaded_at": datetime.now(),
                }
            )

    upsert(cursor, "dim_product", product_rows, "product_id")
    upsert(cursor, "dim_variant", variant_rows, "variant_id")


def load_variant_data(cursor, start_date=None, end_date=None):
    data = to_list(api_get("Variant/Data"))
    rows = []

    for item in data:
        rows.append(
            {
                "variant_id": str(item.get("ID", "")),
                "name": item.get("Name"),
                "code": item.get("Code"),
                "type": item.get("Type"),
                "unit_price": item.get("UnitPrice"),
                "unit_cost": item.get("UnitCost"),
                "_loaded_at": datetime.now(),
            }
        )

    upsert(cursor, "dim_variant_data", rows, "variant_id")


def load_suppliers(cursor, start_date=None, end_date=None):
    data = paginate_get("Supplier", page_size=1000)
    rows = []

    for item in data:
        rows.append(
            {
                "supplier_id": str(item.get("ID", "")),
                "code": item.get("Code"),
                "name": item.get("Name"),
                "phone": item.get("Phone"),
                "mobile": item.get("Mobile"),
                "email": item.get("Email"),
                "_loaded_at": datetime.now(),
            }
        )

    upsert(cursor, "dim_supplier", rows, "supplier_id")


def load_payment_methods(cursor, start_date=None, end_date=None):
    data = to_list(api_get("PaymentMethod"))
    rows = []

    for item in data:
        rows.append(
            {
                "payment_method_id": str(item.get("ID", "")),
                "name": item.get("Name"),
                "type": item.get("Type"),
                "mdr": item.get("MDR"),
                "is_active": not item.get("Suspended", False),
                "_loaded_at": datetime.now(),
            }
        )

    upsert(cursor, "dim_payment_method", rows, "payment_method_id")


def load_taxes(cursor, start_date=None, end_date=None):
    data = to_list(api_get("Tax"))
    rows = []

    for item in data:
        rows.append(
            {
                "tax_id": str(item.get("ID", "")),
                "name": item.get("Name"),
                "rate": item.get("Rate"),
                "type": item.get("Type"),
                "_loaded_at": datetime.now(),
            }
        )

    upsert(cursor, "dim_tax", rows, "tax_id")


def load_users(cursor, start_date=None, end_date=None):
    data = to_list(api_get("User/List"))
    rows = []

    for item in data:
        rows.append(
            {
                "user_id": str(item.get("ID", "")),
                "login_id": item.get("LoginID"),
                "name": item.get("Name"),
                "email": item.get("Email"),
                "type": item.get("Type"),
                "_loaded_at": datetime.now(),
            }
        )

    upsert(cursor, "dim_user", rows, "user_id")


def load_fact_invoice(cursor, start_date=None, end_date=None):
    """Load invoice headers for outlet and tag analysis."""
    raw_rows = paginate_post(
        "Invoice/MultipleOutlet",
        start_date=start_date,
        end_date=end_date,
    )
    rows = []

    for item in raw_rows:
        rows.append(
            {
                "invoice_id": str(item.get("ID", "")),
                "outlet": item.get("Outlet"),
                "number": item.get("Number"),
                "reference_number": item.get("ReferenceNumber"),
                "customer_id": str(item.get("CustomerID")) if item.get("CustomerID") else None,
                "customer": item.get("Customer"),
                "date": item.get("Date"),
                "due": item.get("Due"),
                "amount": item.get("Amount", 0),
                "payment_status": item.get("Payment"),
                "delivery_status": item.get("Delivery"),
                "fulfillment": item.get("Fulfillment"),
                "tag": item.get("Tag"),
                "sales_order_type": item.get("SalesOrderType"),
                "created": item.get("Created"),
                "_loaded_at": datetime.now(),
            }
        )

    upsert(cursor, "fact_invoice", rows, "invoice_id")

def load_fact_invoice_return(cursor, start_date=None, end_date=None):
    """Load invoice headers for outlet and tag analysis."""
    raw_rows = paginate_post(
        "Invoice/Return",
        start_date=start_date,
        end_date=end_date,
    )
    rows = []

    for item in raw_rows:
        rows.append(
            {
                "invoice_id": str(item.get("ID", "")),
                "outlet": item.get("Outlet"),
                "number": item.get("Number"),
                "reference_number": item.get("ReferenceNumber"),
                "customer_id": str(item.get("CustomerID")) if item.get("CustomerID") else None,
                "customer": item.get("Customer"),
                "date": item.get("Date"),
                "due": item.get("Due"),
                "amount": item.get("Amount", 0),
                "payment_status": item.get("Payment"),
                "delivery_status": item.get("Delivery"),
                "fulfillment": item.get("Fulfillment"),
                "tag": item.get("Tag"),
                "sales_order_type": item.get("SalesOrderType"),
                "created": item.get("Created"),
                "_loaded_at": datetime.now(),
            }
        )

    upsert(cursor, "fact_invoice_return", rows, "invoice_id")


def load_fact_invoice_line(cursor, start_date=None, end_date=None):
    """Load invoice line items by expanding each invoice variant."""
    raw_rows = paginate_post(
        "Invoice/MultipleOutlet/WithVariant",
        start_date=start_date,
        end_date=end_date,
    )
    rows = []

    for item in raw_rows:
        invoice_id = str(item.get("ID", ""))
        outlet = item.get("Outlet")
        tag = item.get("Tag")
        date = item.get("Date")
        customer_id = str(item.get("CustomerID")) if item.get("CustomerID") else None

        for variant in item.get("Variants") or []:
            rows.append(
                {
                    "line_id": f"{invoice_id}_{variant.get('VariantID', '')}",
                    "invoice_id": invoice_id,
                    "outlet": outlet,
                    "tag": tag,
                    "date": date,
                    "customer_id": customer_id,
                    "variant_id": str(variant.get("VariantID", "")),
                    "variant_name": variant.get("Name"),
                    "variant_code": variant.get("Code"),
                    "quantity": variant.get("Quantity", 0),
                    "unit_quantity": variant.get("UnitQuantity", 0),
                    "cost": variant.get("Cost", 0),
                    "price": variant.get("Price", 0),
                    "price_original": variant.get("PriceOriginal", 0),
                    "discount_pct": variant.get("Discount", 0),
                    "discount_amount": variant.get("DiscountAmount", 0),
                    "net_sales": variant.get("Sales", 0),
                    "tax": variant.get("Tax", 0),
                    "commission": variant.get("Commission", 0),
                    "expense": variant.get("Expense", 0),
                    "sales_name": variant.get("SalesName"),
                    "taxable": variant.get("Taxable", False),
                    "loyalty_point": variant.get("LoyaltyPoint", False),
                    "note": variant.get("Note"),
                    "_loaded_at": datetime.now(),
                }
            )

    upsert(cursor, "fact_invoice_line", rows, "line_id")


def load_fact_inventory(cursor, start_date=None, end_date=None):
    """Incrementally load inventory changes using paginated /Inventory/Modified."""
    since = get_latest_loaded_at(cursor, "fact_inventory")
    since_param = since.isoformat(timespec="seconds") if since else None
    if since_param:
        log(f"Loading inventory changes since {since_param}.")
    else:
        log("No existing fact_inventory watermark found. Loading full modified inventory feed.")

    rows = []
    page = 1

    while True:
        params = {
            "PageNumber": page,
            "PageSize": PAGE_SIZE,
        }
        if since_param:
            params["Since"] = since_param

        data = to_list(api_get("Inventory/Modified", params))
        if not data:
            log(f"Inventory modified fetch ended at page {page}.")
            break

        load_time = datetime.now()
        for item in data:
            variant_code = item.get("Code")
            outlet_name = item.get("Outlet", "")
            inv = item.get("I") or {}
            on_hand = inv.get("OnHand", item.get("OnHand", item.get("Inventory", 0)))
            allocated = inv.get("Allocated", item.get("Allocated", 0))
            available = inv.get("Available", item.get("Available", on_hand - allocated))

            rows.append(
                {
                    "inventory_id": f"{variant_code}_{outlet_name.replace(' ', '_')}",
                    "variant_code": variant_code,
                    "outlet": outlet_name,
                    "on_hand": on_hand,
                    "allocated": allocated,
                    "available": available,
                    "_loaded_at": load_time,
                }
            )

        log(f"Inventory modified page {page}: {len(data)} rows.")

        if len(data) < PAGE_SIZE:
            break

        page += 1
        time.sleep(0.5)

    return upsert(cursor, "fact_inventory", rows, "inventory_id")


LOADER_CONFIGS = {
    "outlets": {
        "name": "Outlets",
        "callable": load_outlets,
        "supports_incremental_window": False,
        "source_mode": "full_resync",
    },
    "categories": {
        "name": "Categories",
        "callable": load_categories,
        "supports_incremental_window": False,
        "source_mode": "full_resync",
    },
    "customers": {
        "name": "Customers",
        "callable": load_customers,
        "supports_incremental_window": False,
        "source_mode": "full_resync",
    },
    "products": {
        "name": "Products and Variants",
        "callable": load_products,
        "supports_incremental_window": False,
        "source_mode": "full_resync",
    },
    "variant_data": {
        "name": "Variant Data",
        "callable": load_variant_data,
        "supports_incremental_window": False,
        "source_mode": "full_resync",
    },
    "suppliers": {
        "name": "Suppliers",
        "callable": load_suppliers,
        "supports_incremental_window": False,
        "source_mode": "full_resync",
    },
    "payment_methods": {
        "name": "Payment Methods",
        "callable": load_payment_methods,
        "supports_incremental_window": False,
        "source_mode": "full_resync",
    },
    "taxes": {
        "name": "Taxes",
        "callable": load_taxes,
        "supports_incremental_window": False,
        "source_mode": "full_resync",
    },
    "users": {
        "name": "Users",
        "callable": load_users,
        "supports_incremental_window": False,
        "source_mode": "full_resync",
    },
    "fact_invoice": {
        "name": "Fact Invoice",
        "callable": load_fact_invoice,
        "supports_incremental_window": True,
        "source_mode": "rolling_window",
    },
    "fact_invoice_line": {
        "name": "Fact Invoice Line",
        "callable": load_fact_invoice_line,
        "supports_incremental_window": True,
        "source_mode": "rolling_window",
    },
    "fact_invoice_return": {
        "name": "Fact Invoice Return",
        "callable": load_fact_invoice_return,
        "supports_incremental_window": True,
        "source_mode": "rolling_window",
    },
    "fact_inventory": {
        "name": "Fact Inventory",
        "callable": load_fact_inventory,
        "supports_incremental_window": False,
        "source_mode": "full_resync",
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
    global TOKEN

    if loader_key not in LOADER_CONFIGS:
        raise ValueError(f"Unknown loader key: {loader_key}")

    loader_config = LOADER_CONFIGS[loader_key]
    loader_name = loader_config["name"]
    loader = loader_config["callable"]
    source_mode = loader_config["source_mode"]

    log(f"Getting token for loader: {loader_name}")
    TOKEN = get_token()

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


# def main():
#     initial_loader_keys = [
#         "fact_inventory",
#         # "fact_invoice_return"
#         # "outlets",
#         # "categories",
#         # "customers",
#         # "products",
#         # "variant_data",
#         # "suppliers",
#         # "payment_methods",
#         # "taxes",
#         # "users",
#     ]

#     for loader_key in initial_loader_keys:
#         run_loader(loader_key)

#     log("Initial dimension loading finished.")


# if __name__ == "__main__":
#     main()
