"""
Hijack Sandals — Dimension Loader
Fetches from DealPOS API and upserts into PostgreSQL
"""

import requests
import psycopg2
import psycopg2.extras
import os
from datetime import datetime
import time

# ─── Config ──────────────────────────────────────────────────
BASE_URL      = 'https://hijacksandals.dealpos.net/api/v3'
CLIENT_ID     = '019c9a25-05c4-7a38-b04a-dd1efe4c08ef'
CLIENT_SECRET = '019c9a26-51bf-7232-ae0a-4c702ba345e9'

DB_CONFIG = {
    'host':     os.getenv('ETL_DB_HOST', 'localhost'),
    'port':     int(os.getenv('ETL_DB_PORT', '5432')),
    'dbname':   os.getenv('ETL_DB_NAME', 'devs'),
    'user':     os.getenv('ETL_DB_USER', 'supersu'),
    'password': os.getenv('ETL_DB_PASSWORD', 'RRC@2026')
}

# Date range — adjust as needed
DATE_START = '2026-03-01'
DATE_END   = datetime.today().strftime('%Y-%m-%d')
PAGE_SIZE  = 1000

# ─── Auth ────────────────────────────────────────────────────
def get_token():
    resp = requests.post(
        f'{BASE_URL}/Token/OAuth2',
        json={'client_id': CLIENT_ID, 'client_secret': CLIENT_SECRET},
        headers={'Content-Type': 'application/json'}
    )
    resp.raise_for_status()
    data = resp.json()
    token = f"{data['token_type']} {data['access_token']}"
    print(f'✅ Token acquired: {token[:30]}...')
    return token

# ─── API helpers ─────────────────────────────────────────────
def api_get(endpoint, params=None, retries=3):
    for attempt in range(retries):
        resp = requests.get(
            f'{BASE_URL}/{endpoint}',
            headers={'Authorization': TOKEN, 'Accept': '*/*'},
            params=params
        )
        if resp.status_code == 429:
            try:
                wait = int(''.join(filter(str.isdigit, resp.json().get('Message', '')))) or 180
            except:
                wait = 180
            print(f'  ⏳ Rate limited. Waiting {wait}s...')
            time.sleep(wait + 2)
            continue
        if not resp.ok:
            print(f'  ⚠️  {resp.status_code} {resp.url} — {resp.text[:200]}')
            resp.raise_for_status()
        return resp.json()
    raise Exception(f'Failed after {retries} retries: {endpoint}')

def api_post(endpoint, body, retries=3):
    for attempt in range(retries):
        resp = requests.post(
            f'{BASE_URL}/{endpoint}',
            headers={'Authorization': TOKEN, 'Accept': '*/*', 'Content-Type': 'application/json'},
            json=body
        )
        if resp.status_code == 429:
            try:
                wait = int(''.join(filter(str.isdigit, resp.json().get('Message', '')))) or 180
            except:
                wait = 180
            print(f'  ⏳ Rate limited. Waiting {wait}s...')
            time.sleep(wait + 2)
            continue
        if not resp.ok:
            print(f'  ⚠️  {resp.status_code} {endpoint} — {resp.text[:200]}')
            resp.raise_for_status()
        return resp.json()
    raise Exception(f'Failed after {retries} retries: {endpoint}')

# ─── Helpers ─────────────────────────────────────────────────
def to_list(data):
    if isinstance(data, list):
        return data
    elif isinstance(data, dict):
        return [data]
    return []

def paginate_post(endpoint, extra_body=None):
    """Paginate through POST endpoint, return all rows"""
    all_rows = []
    page     = 1
    while True:
        body = {
            'StartDate':  DATE_START,
            'EndDate':    DATE_END,
            'PageNumber': page,
            'PageSize':   PAGE_SIZE,
        }
        if extra_body:
            body.update(extra_body)

        data = to_list(api_post(endpoint, body))
        if not data:
            break

        all_rows.extend(data)
        print(f'  → Page {page}: {len(data)} rows (total: {len(all_rows)})')

        if len(data) < PAGE_SIZE:
            break

        page += 1
        time.sleep(1)

    return all_rows

def safe_ts(val):
    """Return None for out-of-range or null timestamps"""
    if not val:
        return None
    try:
        dt = datetime.fromisoformat(val)
        # PostgreSQL timestamp range: 4713 BC to 294276 AD
        # but psycopg2 chokes on year 0001
        if dt.year < 1970:
            return None
        return val
    except:
        return None

def upsert(cur, table, rows, pk):
    if not rows:
        print(f'  No data for {table}')
        return
    cols         = list(rows[0].keys())
    placeholders = ', '.join(['%s'] * len(cols))
    col_names    = ', '.join(cols)
    updates      = ', '.join([f'{c}=EXCLUDED.{c}' for c in cols if c != pk])
    sql = f"""
        INSERT INTO {table} ({col_names})
        VALUES ({placeholders})
        ON CONFLICT ({pk}) DO UPDATE SET {updates}
    """
    values = [tuple(r.get(c) for c in cols) for r in rows]
    psycopg2.extras.execute_batch(cur, sql, values)
    print(f'  ✅ {table}: {len(rows)} rows upserted')

def paginate_post(endpoint, extra_body=None, max_pages=None):
    """Paginate through POST endpoint, return all rows.
 
    Args:
        max_pages: hard stop after N pages (e.g. max_pages=5 for testing)
                   set to None for no limit (fetch all)
    """
    all_rows = []
    page     = 1
    while True:
        body = {
            'StartDate':  DATE_START,
            'EndDate':    DATE_END,
            'PageNumber': page,
            'PageSize':   PAGE_SIZE,
        }
        if extra_body:
            body.update(extra_body)
 
        data = to_list(api_post(endpoint, body))
        if not data:
            break
 
        all_rows.extend(data)
        print(f'  → Page {page}: {len(data)} rows (total: {len(all_rows)})')
 
        if len(data) < PAGE_SIZE:
            break
 
        if max_pages and page >= max_pages:
            print(f'  ⚠️  Reached max_pages limit ({max_pages}), stopping.')
            break
 
        page += 1
        time.sleep(1)
 
    return all_rows

# ─── Loaders ─────────────────────────────────────────────────

def load_outlets(cur):
    master = to_list(api_get('Outlet', {'Suspended': 'false'}))
    print(f'  Found {len(master)} outlets')
    rows = []
    for i, outlet in enumerate(master):
        code = outlet.get('Code', '')
        name = outlet.get('Name', '')
        try:
            detail = to_list(api_get('Outlet/Detail', {'Code': code, 'Name': name}))
            d  = detail[0]
            rt = d.get('ReceiptTemplate') or {}
            rows.append({
                'outlet_id':         str(d.get('ID', '')),
                'code':              d.get('Code'),
                'name':              d.get('Name'),
                'email':             d.get('Email'),
                'sales_target':      d.get('SalesTarget', 0),
                'minimum_inventory': d.get('MinimumInventory', 0),
                'maximum_inventory': d.get('maximumInventory', 0),
                'order_display':     d.get('OrderDisplayBroadcastMode'),
                'is_suspended':      d.get('Suspended', False),
                'receipt_code':      rt.get('Code'),
                'outlet_name':       rt.get('OutletName'),
                'address':           rt.get('Address'),
                'contact_info':      rt.get('ContactInfo'),
                '_loaded_at':        datetime.now()
            })
            print(f'  → [{i+1}/{len(master)}] {code} ✅')
        except Exception as e:
            print(f'  ⚠️  Skipping {code}: {e}')
        time.sleep(1)
    upsert(cur, 'dim_outlet', rows, 'outlet_id')


def load_categories(cur):
    # Keys: ID, Name, Discontinued
    data = to_list(api_get('Category'))
    rows = []
    for d in data:
        rows.append({
            'category_id': str(d.get('ID', '')),
            'name':        d.get('Name'),
            'is_active':   not d.get('Discontinued', False),
            '_loaded_at':  datetime.now()
        })
    upsert(cur, 'dim_category', rows, 'category_id')


def load_customers(cur):
    # Keys: ID, Name, Code, Mobile, Email, JoinDate, BirthDate,
    #       FirstName, LastName, NationalIDNumber, NationalityID,
    #       Phone, ExpiredDate, StateID
    # Requires pagination params despite no docs mention
    all_rows = []
    page     = 1
    while True:
        data = to_list(api_get('Customer', {'PageNumber': page, 'PageSize': 1000}))
        if not data:
            break
        for d in data:
            all_rows.append({
                'customer_id':        str(d.get('ID', '')),
                'code':               d.get('Code'),
                'name':               d.get('Name'),
                'first_name':         d.get('FirstName'),
                'last_name':          d.get('LastName'),
                'email':              d.get('Email'),
                'mobile':             d.get('Mobile'),
                'phone':              d.get('Phone'),
                'birth_date':         d.get('BirthDate'),
                'join_date':          d.get('JoinDate'),
                'expired_date':       d.get('ExpiredDate'),
                'national_id_number': d.get('NationalIDNumber'),
                'nationality_id':     d.get('NationalityID'),
                'state_id':           d.get('StateID'),
                '_loaded_at':         datetime.now()
            })
        print(f'  → Page {page}: {len(data)} rows')
        if len(data) < 1000:
            break
        page += 1
        time.sleep(1)
    upsert(cur, 'dim_customer', all_rows, 'customer_id')


def load_products(cur):
    # Returns {DataArray: [...], RecordsCount: N}
    # Also explodes nested Variants into dim_variant
    resp     = api_get('Product',{'PageNumber':1,'PageSize':100000000})
    products = resp.get('DataArray', []) if isinstance(resp, dict) else to_list(resp)
    print(f'  Found {len(products)} products')

    prod_rows    = []
    variant_rows = []

    for d in products:
        product_id = str(d.get('ID', ''))
        prod_rows.append({
            'product_id':    product_id,
            'code':          d.get('Code'),
            'name':          d.get('Name'),
            'category':      d.get('Category'),
            'released':      d.get('Released'),
            'thumbnail_url': d.get('ThumbnailUrl'),
            'image_url':     d.get('ImageUrl'),
            'is_active':     not d.get('Discontinued', False),
            'created_at':    d.get('Created'),
            '_loaded_at':    datetime.now()
        })
        for v in (d.get('Variants') or []):
            variant_rows.append({
                'variant_id': str(v.get('ID', '')),
                'product_id': product_id,
                'code':       v.get('Code'),
                'model':      v.get('Model'),
                'unit_price': v.get('UnitPrice'),
                'discount':   v.get('Discount'),
                'weight':     v.get('Weight'),
                'type_id':    v.get('TypeID'),
                'is_active':  not v.get('Discontinued', False),
                '_loaded_at': datetime.now()
            })

    upsert(cur, 'dim_product', prod_rows, 'product_id')
    upsert(cur, 'dim_variant', variant_rows, 'variant_id')


def load_variant_data(cur):
    # Keys: ID, Name, Code, Type, UnitPrice, UnitCost
    data = to_list(api_get('Variant/Data'))
    rows = []
    for d in data:
        rows.append({
            'variant_id': str(d.get('ID', '')),
            'name':       d.get('Name'),
            'code':       d.get('Code'),
            'type':       d.get('Type'),
            'unit_price': d.get('UnitPrice'),
            'unit_cost':  d.get('UnitCost'),
            '_loaded_at': datetime.now()
        })
    upsert(cur, 'dim_variant_data', rows, 'variant_id')


def load_suppliers(cur):
    # Keys: ID, Name, Code, Phone, Mobile, Email
    data = to_list(api_get('Supplier',{'PageNumber':1,'PageSize':100000000}))
    rows = []
    for d in data:
        rows.append({
            'supplier_id': str(d.get('ID', '')),
            'code':        d.get('Code'),
            'name':        d.get('Name'),
            'phone':       d.get('Phone'),
            'mobile':      d.get('Mobile'),
            'email':       d.get('Email'),
            '_loaded_at':  datetime.now()
        })
    upsert(cur, 'dim_supplier', rows, 'supplier_id')


def load_payment_methods(cur):
    # Keys: ID, Name, Type, Suspended, MDR
    data = to_list(api_get('PaymentMethod'))
    rows = []
    for d in data:
        rows.append({
            'payment_method_id': str(d.get('ID', '')),
            'name':              d.get('Name'),
            'type':              d.get('Type'),
            'mdr':               d.get('MDR'),
            'is_active':         not d.get('Suspended', False),
            '_loaded_at':        datetime.now()
        })
    upsert(cur, 'dim_payment_method', rows, 'payment_method_id')


def load_taxes(cur):
    # Keys: ID, Name, Rate, Type
    data = to_list(api_get('Tax'))
    rows = []
    for d in data:
        rows.append({
            'tax_id':    str(d.get('ID', '')),
            'name':      d.get('Name'),
            'rate':      d.get('Rate'),
            'type':      d.get('Type'),
            '_loaded_at': datetime.now()
        })
    upsert(cur, 'dim_tax', rows, 'tax_id')


def load_users(cur):
    # Keys: LoginID, Email, Type, ID, Name
    data = to_list(api_get('User/List'))
    rows = []
    for d in data:
        rows.append({
            'user_id':   str(d.get('ID', '')),
            'login_id':  d.get('LoginID'),
            'name':      d.get('Name'),
            'email':     d.get('Email'),
            'type':      d.get('Type'),
            '_loaded_at': datetime.now()
        })
    upsert(cur, 'dim_user', rows, 'user_id')


def load_fact_invoice(cur):
    """
    GET /Invoice/MultipleOutlet (POST)
    Covers: outlet performance, channel/tag analysis
    Keys: ID, Outlet, Number, CustomerID, Customer, Date, Due,
          Amount, Payment, Delivery, Fulfillment, Created, Tag, SalesOrderType
    """
    raw = paginate_post('Invoice/MultipleOutlet')
    rows = []
    for d in raw:
        rows.append({
            'invoice_id':       str(d.get('ID', '')),
            'outlet':           d.get('Outlet'),
            'number':           d.get('Number'),
            'reference_number': d.get('ReferenceNumber'),
            'customer_id':      str(d.get('CustomerID')) if d.get('CustomerID') else None,
            'customer':         d.get('Customer'),
            'date':             d.get('Date'),
            'due':              d.get('Due'),
            'amount':           d.get('Amount', 0),
            'payment_status':   d.get('Payment'),
            'delivery_status':  d.get('Delivery'),
            'fulfillment':      d.get('Fulfillment'),
            'tag':              d.get('Tag'),
            'sales_order_type': d.get('SalesOrderType'),
            'created':          d.get('Created'),
            '_loaded_at':       datetime.now()
        })
    upsert(cur, 'fact_invoice', rows, 'invoice_id')


def load_fact_invoice_line(cur):
    """
    GET /Invoice/MultipleOutlet/WithVariant (POST)
    Covers: sales by product, best seller ranking
    Explodes nested Variants array into one row per line item
    Keys: invoice header + Variants[].VariantID/Name/Code/Quantity/Price/Sales/Discount etc
    """
    raw = paginate_post('Invoice/MultipleOutlet/WithVariant')
    rows = []
    for d in raw:
        invoice_id = str(d.get('ID', ''))
        outlet     = d.get('Outlet')
        tag        = d.get('Tag')
        date       = d.get('Date')
        customer_id = str(d.get('CustomerID')) if d.get('CustomerID') else None

        for v in (d.get('Variants') or []):
            rows.append({
                'line_id':          f"{invoice_id}_{v.get('VariantID', '')}",
                'invoice_id':       invoice_id,
                'outlet':           outlet,
                'tag':              tag,
                'date':             date,
                'customer_id':      customer_id,
                'variant_id':       str(v.get('VariantID', '')),
                'variant_name':     v.get('Name'),
                'variant_code':     v.get('Code'),
                'quantity':         v.get('Quantity', 0),
                'unit_quantity':    v.get('UnitQuantity', 0),
                'cost':             v.get('Cost', 0),
                'price':            v.get('Price', 0),
                'price_original':   v.get('PriceOriginal', 0),
                'discount_pct':     v.get('Discount', 0),
                'discount_amount':  v.get('DiscountAmount', 0),
                'net_sales':        v.get('Sales', 0),
                'tax':              v.get('Tax', 0),
                'commission':       v.get('Commission', 0),
                'expense':          v.get('Expense', 0),
                'sales_name':       v.get('SalesName'),
                'taxable':          v.get('Taxable', False),
                'loyalty_point':    v.get('LoyaltyPoint', False),
                'note':             v.get('Note'),
                '_loaded_at':       datetime.now()
            })

    upsert(cur, 'fact_invoice_line', rows, 'line_id')


def load_fact_inventory(cur):
    """
    GET /Inventory/CodeArrayGroupByOutlet
    Covers: inventory per outlet
    Requires variant codes — we pull them from dim_variant

    Response: [{Code, Inventories: [{Outlet, Inventory, I: {OnHand, Allocated, Available}}]}]
    """
    # Step 1 — get all variant codes from dim_variant
    conn_inner = psycopg2.connect(**DB_CONFIG)
    cur_inner  = conn_inner.cursor()
    cur_inner.execute("SELECT code FROM dim_variant WHERE code IS NOT NULL")
    codes = [row[0] for row in cur_inner.fetchall()]
    cur_inner.close()
    conn_inner.close()
    print(f'  Found {len(codes)} variant codes')

    if not codes:
        print('  ⚠️  No variant codes found — run load_dimensions first')
        return

    # Step 2 — batch codes (API may have URL length limits)
    BATCH = 50
    rows  = []
    for i in range(0, len(codes), BATCH):
        batch      = codes[i:i+BATCH]
        code_param = ','.join(batch)
        try:
            data = to_list(api_get('Inventory/CodeArrayGroupByOutlet', {'CodeArray': code_param}))
            for d in data:
                variant_code = d.get('Code')
                for inv in (d.get('Inventories') or []):
                    i_obj = inv.get('I') or {}
                    rows.append({
                        'inventory_id':  f"{variant_code}_{inv.get('Outlet', '').replace(' ', '_')}",
                        'variant_code':  variant_code,
                        'outlet':        inv.get('Outlet'),
                        'on_hand':       i_obj.get('OnHand', inv.get('Inventory', 0)),
                        'allocated':     i_obj.get('Allocated', 0),
                        'available':     i_obj.get('Available', 0),
                        '_loaded_at':    datetime.now()
                    })
        except Exception as e:
            print(f'  ⚠️  Batch {i//BATCH+1} failed: {e}')
        time.sleep(0.5)
        print(f'  → Batch {i//BATCH+1}/{(len(codes)+BATCH-1)//BATCH} done')

    upsert(cur, 'fact_inventory', rows, 'inventory_id')

def load_fact_invoice(cur):
    """
    GET /Invoice/MultipleOutlet (POST)
    Covers: outlet performance, channel/tag analysis
    Keys: ID, Outlet, Number, CustomerID, Customer, Date, Due,
          Amount, Payment, Delivery, Fulfillment, Created, Tag, SalesOrderType
    """
    raw = paginate_post('Invoice/MultipleOutlet',max_pages=5)
    rows = []
    for d in raw:
        rows.append({
            'invoice_id':       str(d.get('ID', '')),
            'outlet':           d.get('Outlet'),
            'number':           d.get('Number'),
            'reference_number': d.get('ReferenceNumber'),
            'customer_id':      str(d.get('CustomerID')) if d.get('CustomerID') else None,
            'customer':         d.get('Customer'),
            'date':             d.get('Date'),
            'due':              d.get('Due'),
            'amount':           d.get('Amount', 0),
            'payment_status':   d.get('Payment'),
            'delivery_status':  d.get('Delivery'),
            'fulfillment':      d.get('Fulfillment'),
            'tag':              d.get('Tag'),
            'sales_order_type': d.get('SalesOrderType'),
            'created':          d.get('Created'),
            '_loaded_at':       datetime.now()
        })
    upsert(cur, 'fact_invoice', rows, 'invoice_id')


def load_fact_invoice_line(cur):
    """
    GET /Invoice/MultipleOutlet/WithVariant (POST)
    Covers: sales by product, best seller ranking
    Explodes nested Variants array into one row per line item
    Keys: invoice header + Variants[].VariantID/Name/Code/Quantity/Price/Sales/Discount etc
    """
    raw = paginate_post('Invoice/MultipleOutlet/WithVariant',max_pages=5)
    rows = []
    for d in raw:
        invoice_id = str(d.get('ID', ''))
        outlet     = d.get('Outlet')
        tag        = d.get('Tag')
        date       = d.get('Date')
        customer_id = str(d.get('CustomerID')) if d.get('CustomerID') else None

        for v in (d.get('Variants') or []):
            rows.append({
                'line_id':          f"{invoice_id}_{v.get('VariantID', '')}",
                'invoice_id':       invoice_id,
                'outlet':           outlet,
                'tag':              tag,
                'date':             date,
                'customer_id':      customer_id,
                'variant_id':       str(v.get('VariantID', '')),
                'variant_name':     v.get('Name'),
                'variant_code':     v.get('Code'),
                'quantity':         v.get('Quantity', 0),
                'unit_quantity':    v.get('UnitQuantity', 0),
                'cost':             v.get('Cost', 0),
                'price':            v.get('Price', 0),
                'price_original':   v.get('PriceOriginal', 0),
                'discount_pct':     v.get('Discount', 0),
                'discount_amount':  v.get('DiscountAmount', 0),
                'net_sales':        v.get('Sales', 0),
                'tax':              v.get('Tax', 0),
                'commission':       v.get('Commission', 0),
                'expense':          v.get('Expense', 0),
                'sales_name':       v.get('SalesName'),
                'taxable':          v.get('Taxable', False),
                'loyalty_point':    v.get('LoyaltyPoint', False),
                'note':             v.get('Note'),
                '_loaded_at':       datetime.now()
            })

    upsert(cur, 'fact_invoice_line', rows, 'line_id')


def load_fact_inventory(cur):
    """
    GET /Inventory/CodeArrayGroupByOutlet
    Covers: inventory per outlet
    Requires variant codes — we pull them from dim_variant
 
    Response: [{Code, Inventories: [{Outlet, Inventory, I: {OnHand, Allocated, Available}}]}]
    """
    # Step 1 — get all variant codes from dim_variant
    conn_inner = psycopg2.connect(**DB_CONFIG)
    cur_inner  = conn_inner.cursor()
    cur_inner.execute("SELECT code FROM dim_variant WHERE code IS NOT NULL")
    codes = [row[0] for row in cur_inner.fetchall()]
    cur_inner.close()
    conn_inner.close()
    print(f'  Found {len(codes)} variant codes')
 
    if not codes:
        print('  ⚠️  No variant codes found — run load_dimensions first')
        return
 
    # Step 2 — batch codes (API may have URL length limits)
    BATCH = 50
    rows  = []
    for i in range(0, len(codes), BATCH):
        batch      = codes[i:i+BATCH]
        code_param = ','.join(batch)
        try:
            data = to_list(api_post('Inventory/CodeArrayGroupByOutlet', {'Code': batch}))  # batch is already a list
            for d in data:
                variant_code = d.get('Code')
                for inv in (d.get('Inventories') or []):
                    i_obj = inv.get('I') or {}
                    rows.append({
                        'inventory_id':  f"{variant_code}_{inv.get('Outlet', '').replace(' ', '_')}",
                        'variant_code':  variant_code,
                        'outlet':        inv.get('Outlet'),
                        'on_hand':       i_obj.get('OnHand', inv.get('Inventory', 0)),
                        'allocated':     i_obj.get('Allocated', 0),
                        'available':     i_obj.get('Available', 0),
                        '_loaded_at':    datetime.now()
                    })
        except Exception as e:
            print(f'  ⚠️  Batch {i//BATCH+1} failed: {e}')
        time.sleep(0.5)
        print(f'  → Batch {i//BATCH+1}/{(len(codes)+BATCH-1)//BATCH} done')
 
    upsert(cur, 'fact_inventory', rows, 'inventory_id')

def load_fact_invoice_return(cur):
    """
    POST /Invoice/Return
    Covers: voided/returned invoices — use to exclude from sales reports
    Keys: ID, Outlet, Number, Customer, Date, EventDate, Due,
          Amount, Payment, Fulfillment, Created
    """
    raw = paginate_post('Invoice/Return')
    rows = []
    for d in raw:
        rows.append({
            'invoice_id':     str(d.get('ID', '')),
            'outlet':         d.get('Outlet'),
            'number':         d.get('Number'),
            'customer':       d.get('Customer'),
            'date':           safe_ts(d.get('Date')),
            'event_date':     safe_ts(d.get('EventDate')),
            'due':            safe_ts(d.get('Due')),
            'amount':         d.get('Amount', 0),
            'payment_status': d.get('Payment'),
            'fulfillment':    d.get('Fulfillment'),
            'created':        safe_ts(d.get('Created')),
            '_loaded_at':     datetime.now()
        })
    upsert(cur, 'fact_invoice_return', rows, 'invoice_id')

# ─── Main ────────────────────────────────────────────────────
def main():
    global TOKEN

    print('🔑 Getting token...')
    TOKEN = get_token()

    print('🔌 Connecting to PostgreSQL...')
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()

    loaders = [
        ('Outlets',           load_outlets),         
        ('Categories',          load_categories),
        ('Customers',           load_customers),
        ('Products+Variants',   load_products),
        ('Variant Data',        load_variant_data),
        ('Suppliers',           load_suppliers),
        ('Payment Methods',     load_payment_methods),
        ('Taxes',               load_taxes),
        ('Users',               load_users),
        ('Invoice Header',    load_fact_invoice),
        ('Invoice Line Items',load_fact_invoice_line),
        ('Inventory',         load_fact_inventory),
        ('Invoice Returns',    load_fact_invoice_return),
    ]

    for name, loader in loaders:
        print(f'\n📦 Loading {name}...')
        try:
            loader(cur)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f'  ❌ Failed: {e}')

    cur.close()
    conn.close()
    print('\n✅ All dimensions loaded!')

if __name__ == '__main__':
    main()
