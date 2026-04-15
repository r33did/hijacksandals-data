-- public.dim_category definition

-- Drop table

-- DROP TABLE public.dim_category;

CREATE TABLE public.dim_category (
	category_id varchar(50) NOT NULL,
	"name" varchar(255) NULL,
	is_active bool DEFAULT true NULL,
	_loaded_at timestamp DEFAULT now() NULL,
	CONSTRAINT dim_category_pkey PRIMARY KEY (category_id)
);


-- public.dim_customer definition

-- Drop table

-- DROP TABLE public.dim_customer;

CREATE TABLE public.dim_customer (
	customer_id varchar(50) NOT NULL,
	code varchar(100) NULL,
	"name" varchar(255) NULL,
	email varchar(255) NULL,
	mobile_phone varchar(50) NULL,
	address text NULL,
	gender varchar(20) NULL,
	birth_date date NULL,
	loyalty_point numeric(18, 2) NULL,
	is_suspended bool DEFAULT false NULL,
	created_at timestamp NULL,
	_loaded_at timestamp DEFAULT now() NULL,
	CONSTRAINT dim_customer_pkey PRIMARY KEY (customer_id)
);


-- public.dim_outlet definition

-- Drop table

-- DROP TABLE public.dim_outlet;

CREATE TABLE public.dim_outlet (
	outlet_id varchar(50) NOT NULL,
	code varchar(50) NULL,
	"name" varchar(255) NULL,
	email varchar(255) NULL,
	outlet_name varchar(255) NULL,
	address text NULL,
	contact_info text NULL,
	receipt_code varchar(100) NULL,
	sales_target numeric(18, 2) NULL,
	minimum_inventory int4 NULL,
	maximum_inventory int4 NULL,
	order_display int4 NULL,
	is_suspended bool DEFAULT false NULL,
	_loaded_at timestamp DEFAULT now() NULL,
	CONSTRAINT dim_outlet_pkey PRIMARY KEY (outlet_id)
);


-- public.dim_payment_method definition

-- Drop table

-- DROP TABLE public.dim_payment_method;

CREATE TABLE public.dim_payment_method (
	payment_method_id varchar(50) NOT NULL,
	"name" varchar(255) NULL,
	"type" varchar(100) NULL,
	mdr numeric(8, 4) NULL,
	is_active bool DEFAULT true NULL,
	_loaded_at timestamp DEFAULT now() NULL,
	CONSTRAINT dim_payment_method_pkey PRIMARY KEY (payment_method_id)
);


-- public.dim_pricebook definition

-- Drop table

-- DROP TABLE public.dim_pricebook;

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

-- DROP TABLE public.dim_product;

CREATE TABLE public.dim_product (
	product_id varchar(50) NOT NULL,
	code varchar(100) NULL,
	"name" varchar(255) NULL,
	category varchar(255) NULL,
	released timestamp NULL,
	thumbnail_url text NULL,
	image_url text NULL,
	is_active bool DEFAULT true NULL,
	created_at timestamp NULL,
	_loaded_at timestamp DEFAULT now() NULL,
	CONSTRAINT dim_product_pkey PRIMARY KEY (product_id)
);


-- public.dim_supplier definition

-- Drop table

-- DROP TABLE public.dim_supplier;

CREATE TABLE public.dim_supplier (
	supplier_id varchar(50) NOT NULL,
	code varchar(100) NULL,
	"name" varchar(255) NULL,
	phone varchar(50) NULL,
	mobile varchar(50) NULL,
	email varchar(255) NULL,
	_loaded_at timestamp DEFAULT now() NULL,
	CONSTRAINT dim_supplier_pkey PRIMARY KEY (supplier_id)
);


-- public.dim_tax definition

-- Drop table

-- DROP TABLE public.dim_tax;

CREATE TABLE public.dim_tax (
	tax_id varchar(50) NOT NULL,
	"name" varchar(255) NULL,
	rate numeric(8, 4) NULL,
	"type" varchar(100) NULL,
	_loaded_at timestamp DEFAULT now() NULL,
	CONSTRAINT dim_tax_pkey PRIMARY KEY (tax_id)
);


-- public.dim_user definition

-- Drop table

-- DROP TABLE public.dim_user;

CREATE TABLE public.dim_user (
	user_id varchar(50) NOT NULL,
	login_id varchar(255) NULL,
	"name" varchar(255) NULL,
	email varchar(255) NULL,
	"type" varchar(100) NULL,
	_loaded_at timestamp DEFAULT now() NULL,
	CONSTRAINT dim_user_pkey PRIMARY KEY (user_id)
);


-- public.dim_variant_data definition

-- Drop table

-- DROP TABLE public.dim_variant_data;

CREATE TABLE public.dim_variant_data (
	variant_id varchar(50) NOT NULL,
	"name" varchar(255) NULL,
	code varchar(100) NULL,
	"type" varchar(100) NULL,
	unit_price numeric(18, 2) NULL,
	unit_cost numeric(18, 2) NULL,
	_loaded_at timestamp DEFAULT now() NULL,
	CONSTRAINT dim_variant_data_pkey PRIMARY KEY (variant_id)
);


-- public.fact_inventory definition

-- Drop table

-- DROP TABLE public.fact_inventory;

CREATE TABLE public.fact_inventory (
	inventory_id varchar(255) NOT NULL,
	variant_code varchar(100) NULL,
	outlet varchar(255) NULL,
	on_hand numeric(18, 2) NULL,
	allocated numeric(18, 2) NULL,
	available numeric(18, 2) NULL,
	_loaded_at timestamp DEFAULT now() NULL,
	CONSTRAINT fact_inventory_pkey PRIMARY KEY (inventory_id)
);
CREATE INDEX idx_fact_inv_outlet ON public.fact_inventory USING btree (outlet);
CREATE INDEX idx_fact_inv_variant_code ON public.fact_inventory USING btree (variant_code);


-- public.fact_invoice definition

-- Drop table

-- DROP TABLE public.fact_invoice;

CREATE TABLE public.fact_invoice (
	invoice_id varchar(50) NOT NULL,
	outlet varchar(255) NULL,
	"number" varchar(100) NULL,
	reference_number varchar(255) NULL,
	customer_id varchar(50) NULL,
	customer varchar(255) NULL,
	"date" timestamp NULL,
	due timestamp NULL,
	amount numeric(18, 2) NULL,
	payment_status varchar(50) NULL,
	delivery_status varchar(50) NULL,
	fulfillment varchar(50) NULL,
	tag varchar(100) NULL,
	sales_order_type varchar(100) NULL,
	created timestamp NULL,
	_loaded_at timestamp DEFAULT now() NULL,
	CONSTRAINT fact_invoice_pkey PRIMARY KEY (invoice_id)
);
CREATE INDEX idx_fact_invoice_date ON public.fact_invoice USING btree (date);
CREATE INDEX idx_fact_invoice_outlet ON public.fact_invoice USING btree (outlet);
CREATE INDEX idx_fact_invoice_tag ON public.fact_invoice USING btree (tag);


-- public.fact_invoice_line definition

-- Drop table

-- DROP TABLE public.fact_invoice_line;

CREATE TABLE public.fact_invoice_line (
	line_id varchar(150) NOT NULL,
	invoice_id varchar(50) NULL,
	outlet varchar(255) NULL,
	tag varchar(100) NULL,
	"date" timestamp NULL,
	customer_id varchar(50) NULL,
	variant_id varchar(50) NULL,
	variant_name varchar(255) NULL,
	variant_code varchar(100) NULL,
	quantity int4 NULL,
	unit_quantity int4 NULL,
	"cost" numeric(18, 2) NULL,
	price numeric(18, 2) NULL,
	price_original numeric(18, 2) NULL,
	discount_pct numeric(8, 4) NULL,
	discount_amount numeric(18, 2) NULL,
	net_sales numeric(18, 2) NULL,
	tax numeric(18, 2) NULL,
	commission numeric(18, 2) NULL,
	expense numeric(18, 2) NULL,
	sales_name varchar(255) NULL,
	taxable bool NULL,
	loyalty_point bool NULL,
	note text NULL,
	_loaded_at timestamp DEFAULT now() NULL,
	CONSTRAINT fact_invoice_line_pkey PRIMARY KEY (line_id)
);
CREATE INDEX idx_fact_line_date ON public.fact_invoice_line USING btree (date);
CREATE INDEX idx_fact_line_invoice_id ON public.fact_invoice_line USING btree (invoice_id);
CREATE INDEX idx_fact_line_outlet ON public.fact_invoice_line USING btree (outlet);
CREATE INDEX idx_fact_line_tag ON public.fact_invoice_line USING btree (tag);
CREATE INDEX idx_fact_line_variant_code ON public.fact_invoice_line USING btree (variant_code);
CREATE INDEX idx_fact_line_variant_id ON public.fact_invoice_line USING btree (variant_id);


-- public.dim_variant definition

-- Drop table

-- DROP TABLE public.dim_variant;

CREATE TABLE public.dim_variant (
	variant_id varchar(50) NOT NULL,
	product_id varchar(50) NULL,
	code varchar(100) NULL,
	model varchar(50) NULL,
	unit_price numeric(18, 2) NULL,
	discount numeric(18, 2) NULL,
	weight numeric(18, 4) NULL,
	type_id int4 NULL,
	is_active bool DEFAULT true NULL,
	_loaded_at timestamp DEFAULT now() NULL,
	CONSTRAINT dim_variant_pkey PRIMARY KEY (variant_id),
	CONSTRAINT dim_variant_product_id_fkey FOREIGN KEY (product_id) REFERENCES public.dim_product(product_id)
);