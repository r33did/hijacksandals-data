-- ============================================================
-- Hijack Sandals ETL — DDL Statements
-- Generated from loader definitions in loader.py
-- ============================================================


-- ------------------------------------------------------------
-- ETL Sync State (internal)
-- ------------------------------------------------------------

DROP TABLE IF EXISTS etl_sync_state;
CREATE TABLE etl_sync_state (
    loader_key          TEXT PRIMARY KEY,
    loader_name         TEXT NOT NULL,
    last_status         TEXT,
    last_started_at     TIMESTAMP,
    last_finished_at    TIMESTAMP,
    last_success_at     TIMESTAMP,
    last_error          TEXT,
    source_mode         TEXT NOT NULL DEFAULT 'full_resync',
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);


-- ------------------------------------------------------------
-- dim_outlet  (load_outlets)
-- ------------------------------------------------------------

DROP TABLE IF EXISTS dim_outlet;
CREATE TABLE dim_outlet (
    outlet_id           TEXT PRIMARY KEY,
    code                TEXT,
    name                TEXT,
    email               TEXT,
    sales_target        NUMERIC,
    minimum_inventory   NUMERIC,
    maximum_inventory   NUMERIC,
    order_display       TEXT,
    is_suspended        BOOLEAN,
    receipt_code        TEXT,
    outlet_name         TEXT,
    address             TEXT,
    contact_info        TEXT,
    _loaded_at          TIMESTAMP
);


-- ------------------------------------------------------------
-- dim_category  (load_categories)
-- ------------------------------------------------------------

DROP TABLE IF EXISTS dim_category;
CREATE TABLE dim_category (
    category_id     TEXT PRIMARY KEY,
    name            TEXT,
    is_active       BOOLEAN,
    _loaded_at      TIMESTAMP
);


-- ------------------------------------------------------------
-- dim_customer  (load_customers)
-- ------------------------------------------------------------

DROP TABLE IF EXISTS dim_customer;
CREATE TABLE dim_customer (
    customer_id         TEXT PRIMARY KEY,
    code                TEXT,
    name                TEXT,
    first_name          TEXT,
    last_name           TEXT,
    email               TEXT,
    mobile              TEXT,
    phone               TEXT,
    birth_date          TEXT,
    join_date           TEXT,
    expired_date        TEXT,
    national_id_number  TEXT,
    nationality_id      TEXT,
    state_id            TEXT,
    _loaded_at          TIMESTAMP
);


-- ------------------------------------------------------------
-- dim_product  (load_products)
-- ------------------------------------------------------------

DROP TABLE IF EXISTS dim_product;
CREATE TABLE dim_product (
    product_id      TEXT PRIMARY KEY,
    code            TEXT,
    name            TEXT,
    category        TEXT,
    released        TEXT,
    thumbnail_url   TEXT,
    image_url       TEXT,
    is_active       BOOLEAN,
    created_at      TEXT,
    _loaded_at      TIMESTAMP
);


-- ------------------------------------------------------------
-- dim_variant  (load_products)
-- ------------------------------------------------------------

DROP TABLE IF EXISTS dim_variant;
CREATE TABLE dim_variant (
    variant_id      TEXT PRIMARY KEY,
    product_id      TEXT,
    code            TEXT,
    model           TEXT,
    unit_price      NUMERIC,
    discount        NUMERIC,
    weight          NUMERIC,
    type_id         TEXT,
    is_active       BOOLEAN,
    _loaded_at      TIMESTAMP
);


-- ------------------------------------------------------------
-- dim_variant_data  (load_variant_data)
-- ------------------------------------------------------------

DROP TABLE IF EXISTS dim_variant_data;
CREATE TABLE dim_variant_data (
    variant_id      TEXT PRIMARY KEY,
    name            TEXT,
    code            TEXT,
    type            TEXT,
    unit_price      NUMERIC,
    unit_cost       NUMERIC,
    _loaded_at      TIMESTAMP
);


-- ------------------------------------------------------------
-- dim_supplier  (load_suppliers)
-- ------------------------------------------------------------

DROP TABLE IF EXISTS dim_supplier;
CREATE TABLE dim_supplier (
    supplier_id     TEXT PRIMARY KEY,
    code            TEXT,
    name            TEXT,
    phone           TEXT,
    mobile          TEXT,
    email           TEXT,
    _loaded_at      TIMESTAMP
);


-- ------------------------------------------------------------
-- dim_payment_method  (load_payment_methods)
-- ------------------------------------------------------------

DROP TABLE IF EXISTS dim_payment_method;
CREATE TABLE dim_payment_method (
    payment_method_id   TEXT PRIMARY KEY,
    name                TEXT,
    type                TEXT,
    mdr                 NUMERIC,
    is_active           BOOLEAN,
    _loaded_at          TIMESTAMP
);


-- ------------------------------------------------------------
-- dim_tax  (load_taxes)
-- ------------------------------------------------------------

DROP TABLE IF EXISTS dim_tax;
CREATE TABLE dim_tax (
    tax_id      TEXT PRIMARY KEY,
    name        TEXT,
    rate        NUMERIC,
    type        TEXT,
    _loaded_at  TIMESTAMP
);


-- ------------------------------------------------------------
-- dim_user  (load_users)
-- ------------------------------------------------------------

DROP TABLE IF EXISTS dim_user;
CREATE TABLE dim_user (
    user_id     TEXT PRIMARY KEY,
    login_id    TEXT,
    name        TEXT,
    email       TEXT,
    type        TEXT,
    _loaded_at  TIMESTAMP
);


-- ------------------------------------------------------------
-- fact_invoice  (load_fact_invoice)
-- ------------------------------------------------------------

DROP TABLE IF EXISTS fact_invoice;
CREATE TABLE fact_invoice (
    invoice_id          TEXT PRIMARY KEY,
    outlet              TEXT,
    number              TEXT,
    reference_number    TEXT,
    customer_id         TEXT,
    customer            TEXT,
    date                TEXT,
    due                 TEXT,
    amount              NUMERIC,
    payment_status      TEXT,
    delivery_status     TEXT,
    fulfillment         TEXT,
    tag                 TEXT,
    sales_order_type    TEXT,
    created             TEXT,
    _loaded_at          TIMESTAMP
);


-- ------------------------------------------------------------
-- fact_invoice_return  (load_fact_invoice_return)
-- ------------------------------------------------------------

DROP TABLE IF EXISTS fact_invoice_return;
CREATE TABLE fact_invoice_return (
    invoice_id          TEXT PRIMARY KEY,
    outlet              TEXT,
    number              TEXT,
    reference_number    TEXT,
    customer_id         TEXT,
    customer            TEXT,
    date                TEXT,
    due                 TEXT,
    amount              NUMERIC,
    payment_status      TEXT,
    delivery_status     TEXT,
    fulfillment         TEXT,
    tag                 TEXT,
    sales_order_type    TEXT,
    created             TEXT,
    _loaded_at          TIMESTAMP
);


-- ------------------------------------------------------------
-- fact_invoice_line  (load_fact_invoice_line)
-- ------------------------------------------------------------

DROP TABLE IF EXISTS fact_invoice_line;
CREATE TABLE fact_invoice_line (
    line_id             TEXT PRIMARY KEY,
    invoice_id          TEXT,
    outlet              TEXT,
    tag                 TEXT,
    date                TEXT,
    customer_id         TEXT,
    variant_id          TEXT,
    variant_name        TEXT,
    variant_code        TEXT,
    quantity            NUMERIC,
    unit_quantity       NUMERIC,
    cost                NUMERIC,
    price               NUMERIC,
    price_original      NUMERIC,
    discount_pct        NUMERIC,
    discount_amount     NUMERIC,
    net_sales           NUMERIC,
    tax                 NUMERIC,
    commission          NUMERIC,
    expense             NUMERIC,
    sales_name          TEXT,
    taxable             BOOLEAN,
    loyalty_point       BOOLEAN,
    note                TEXT,
    _loaded_at          TIMESTAMP
);


-- ------------------------------------------------------------
-- fact_inventory  (load_fact_inventory)
-- ------------------------------------------------------------

DROP TABLE IF EXISTS fact_inventory;
CREATE TABLE fact_inventory (
    inventory_id    TEXT PRIMARY KEY,
    variant_code    TEXT,
    outlet          TEXT,
    on_hand         NUMERIC,
    allocated       NUMERIC,
    available       NUMERIC,
    _loaded_at      TIMESTAMP
);