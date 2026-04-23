-- public.dim_category definition

-- Drop table

DROP IF EXISTS TABLE public.dim_category;

CREATE TABLE public.dim_category (
	category_id text NOT NULL,
	"name" text NULL,
	is_active bool NULL,
	_loaded_at timestamp NULL,
	CONSTRAINT dim_category_pkey PRIMARY KEY (category_id)
);


-- public.dim_customer definition

-- Drop table

DROP IF EXISTS TABLE public.dim_customer;

CREATE TABLE public.dim_customer (
	customer_id text NOT NULL,
	code text NULL,
	"name" text NULL,
	first_name text NULL,
	last_name text NULL,
	email text NULL,
	mobile text NULL,
	phone text NULL,
	birth_date text NULL,
	join_date text NULL,
	expired_date text NULL,
	national_id_number text NULL,
	nationality_id text NULL,
	state_id text NULL,
	_loaded_at timestamp NULL,
	CONSTRAINT dim_customer_pkey PRIMARY KEY (customer_id)
);


-- public.dim_outlet definition

-- Drop table

DROP IF EXISTS TABLE public.dim_outlet;

CREATE TABLE public.dim_outlet (
	outlet_id text NOT NULL,
	code text NULL,
	"name" text NULL,
	email text NULL,
	sales_target numeric NULL,
	minimum_inventory numeric NULL,
	maximum_inventory numeric NULL,
	order_display text NULL,
	is_suspended bool NULL,
	receipt_code text NULL,
	outlet_name text NULL,
	address text NULL,
	contact_info text NULL,
	_loaded_at timestamp NULL,
	CONSTRAINT dim_outlet_pkey PRIMARY KEY (outlet_id)
);


-- public.dim_payment_method definition

-- Drop table

DROP IF EXISTS TABLE public.dim_payment_method;

CREATE TABLE public.dim_payment_method (
	payment_method_id text NOT NULL,
	"name" text NULL,
	"type" text NULL,
	mdr numeric NULL,
	is_active bool NULL,
	_loaded_at timestamp NULL,
	CONSTRAINT dim_payment_method_pkey PRIMARY KEY (payment_method_id)
);


-- public.dim_pricebook definition

-- Drop table

DROP IF EXISTS TABLE public.dim_pricebook;

CREATE TABLE public.dim_pricebook (
	pricebook_id varchar(50) NOT NULL,
	"name" varchar(255) NULL,
	description text NULL,
	is_active bool DEFAULT true NULL,
	_loaded_at timestamp DEFAULT now() NULL,
	CONSTRAINT dim_pricebook_pkey PRIMARY KEY (pricebook_id)
);


-- public.dim_product definition

-- Drop table

DROP IF EXISTS TABLE public.dim_product;

CREATE TABLE public.dim_product (
	product_id text NOT NULL,
	code text NULL,
	"name" text NULL,
	category text NULL,
	released text NULL,
	thumbnail_url text NULL,
	image_url text NULL,
	is_active bool NULL,
	created_at text NULL,
	_loaded_at timestamp NULL,
	CONSTRAINT dim_product_pkey PRIMARY KEY (product_id)
);


-- public.dim_supplier definition

-- Drop table

DROP IF EXISTS TABLE public.dim_supplier;

CREATE TABLE public.dim_supplier (
	supplier_id text NOT NULL,
	code text NULL,
	"name" text NULL,
	phone text NULL,
	mobile text NULL,
	email text NULL,
	_loaded_at timestamp NULL,
	CONSTRAINT dim_supplier_pkey PRIMARY KEY (supplier_id)
);


-- public.dim_tax definition

-- Drop table

DROP IF EXISTS TABLE public.dim_tax;

CREATE TABLE public.dim_tax (
	tax_id text NOT NULL,
	"name" text NULL,
	rate numeric NULL,
	"type" text NULL,
	_loaded_at timestamp NULL,
	CONSTRAINT dim_tax_pkey PRIMARY KEY (tax_id)
);


-- public.dim_user definition

-- Drop table

DROP IF EXISTS TABLE public.dim_user;

CREATE TABLE public.dim_user (
	user_id text NOT NULL,
	login_id text NULL,
	"name" text NULL,
	email text NULL,
	"type" text NULL,
	_loaded_at timestamp NULL,
	CONSTRAINT dim_user_pkey PRIMARY KEY (user_id)
);


-- public.dim_variant definition

-- Drop table

DROP IF EXISTS TABLE public.dim_variant;

CREATE TABLE public.dim_variant (
	variant_id text NOT NULL,
	product_id text NULL,
	code text NULL,
	model text NULL,
	unit_price numeric NULL,
	discount numeric NULL,
	weight numeric NULL,
	type_id text NULL,
	is_active bool NULL,
	_loaded_at timestamp NULL,
	CONSTRAINT dim_variant_pkey PRIMARY KEY (variant_id)
);


-- public.dim_variant_data definition

-- Drop table

DROP IF EXISTS TABLE public.dim_variant_data;

CREATE TABLE public.dim_variant_data (
	variant_id text NOT NULL,
	"name" text NULL,
	code text NULL,
	"type" text NULL,
	unit_price numeric NULL,
	unit_cost numeric NULL,
	_loaded_at timestamp NULL,
	CONSTRAINT dim_variant_data_pkey PRIMARY KEY (variant_id)
);


-- public.etl_sync_state definition

-- Drop table

DROP IF EXISTS TABLE public.etl_sync_state;

CREATE TABLE public.etl_sync_state (
	loader_key text NOT NULL,
	loader_name text NOT NULL,
	last_status text NULL,
	last_started_at timestamp NULL,
	last_finished_at timestamp NULL,
	last_success_at timestamp NULL,
	last_error text NULL,
	source_mode text DEFAULT 'full_resync'::text NOT NULL,
	updated_at timestamp DEFAULT now() NOT NULL,
	CONSTRAINT etl_sync_state_pkey PRIMARY KEY (loader_key)
);


-- public.fact_inventory definition

-- Drop table

DROP IF EXISTS TABLE public.fact_inventory;

CREATE TABLE public.fact_inventory (
	inventory_id text NOT NULL,
	variant_code text NULL,
	outlet text NULL,
	on_hand numeric NULL,
	allocated numeric NULL,
	available numeric NULL,
	_loaded_at timestamp NULL,
	CONSTRAINT fact_inventory_pkey PRIMARY KEY (inventory_id)
);


-- public.fact_invoice definition

-- Drop table

DROP IF EXISTS TABLE public.fact_invoice;

CREATE TABLE public.fact_invoice (
	invoice_id text NOT NULL,
	outlet text NULL,
	"number" text NULL,
	reference_number text NULL,
	customer_id text NULL,
	customer text NULL,
	"date" text NULL,
	due text NULL,
	amount numeric NULL,
	payment_status text NULL,
	delivery_status text NULL,
	fulfillment text NULL,
	tag text NULL,
	sales_order_type text NULL,
	created text NULL,
	_loaded_at timestamp NULL,
	CONSTRAINT fact_invoice_pkey PRIMARY KEY (invoice_id)
);


-- public.fact_invoice_line definition

-- Drop table

DROP IF EXISTS TABLE public.fact_invoice_line;

CREATE TABLE public.fact_invoice_line (
	line_id text NOT NULL,
	invoice_id text NULL,
	outlet text NULL,
	tag text NULL,
	"date" text NULL,
	customer_id text NULL,
	variant_id text NULL,
	variant_name text NULL,
	variant_code text NULL,
	quantity numeric NULL,
	unit_quantity numeric NULL,
	"cost" numeric NULL,
	price numeric NULL,
	price_original numeric NULL,
	discount_pct numeric NULL,
	discount_amount numeric NULL,
	net_sales numeric NULL,
	tax numeric NULL,
	commission numeric NULL,
	expense numeric NULL,
	sales_name text NULL,
	taxable bool NULL,
	loyalty_point bool NULL,
	note text NULL,
	_loaded_at timestamp NULL,
	CONSTRAINT fact_invoice_line_pkey PRIMARY KEY (line_id)
);


-- public.fact_invoice_return definition

-- Drop table

DROP IF EXISTS TABLE public.fact_invoice_return;

CREATE TABLE public.fact_invoice_return (
	invoice_id text NOT NULL,
	outlet text NULL,
	"number" text NULL,
	reference_number text NULL,
	customer_id text NULL,
	customer text NULL,
	"date" text NULL,
	due text NULL,
	amount numeric NULL,
	payment_status text NULL,
	delivery_status text NULL,
	fulfillment text NULL,
	tag text NULL,
	sales_order_type text NULL,
	created text NULL,
	_loaded_at timestamp NULL,
	CONSTRAINT fact_invoice_return_pkey PRIMARY KEY (invoice_id)
);