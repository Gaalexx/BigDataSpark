CREATE TABLE dim_customer (
    customer_key uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_email text NOT NULL UNIQUE,
    first_name text,
    last_name text,
    age integer,
    country text,
    postal_code text,
    pet_type text,
    pet_name text,
    pet_breed text
);

CREATE TABLE dim_seller (
    seller_key uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    seller_email text NOT NULL UNIQUE,
    first_name text,
    last_name text,
    country text,
    postal_code text
);

CREATE TABLE dim_store (
    store_key uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    store_email text NOT NULL UNIQUE,
    store_name text,
    location text,
    city text,
    state text,
    country text,
    phone text
);

CREATE TABLE dim_supplier (
    supplier_key uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    supplier_email text NOT NULL UNIQUE,
    supplier_name text,
    contact_name text,
    phone text,
    address text,
    city text,
    country text
);

CREATE TABLE dim_product (
     product_key uuid PRIMARY KEY DEFAULT gen_random_uuid(),
     product_name text,
     product_category text,
     pet_category text,
     unit_price numeric(10, 2),
     available_quantity integer,
     weight numeric(10, 2),
     color text,
     size text,
     brand text,
     material text,
     description text,
     rating numeric(3, 1),
     reviews integer,
     release_date date,
     expiry_date date
);

CREATE TABLE fact_sales (
    sales_key uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_raw_id bigint NOT NULL,
    source_raw_key UUID NOT NULL,
    customer_key uuid NOT NULL REFERENCES dim_customer(customer_key),
    seller_key uuid NOT NULL REFERENCES dim_seller(seller_key),
    store_key uuid NOT NULL REFERENCES dim_store(store_key),
    supplier_key uuid NOT NULL REFERENCES dim_supplier(supplier_key),
    product_key uuid NOT NULL REFERENCES dim_product(product_key),
    sale_quantity integer NOT NULL,
    sale_total_price numeric(10, 2) NOT NULL,
    source_unit_price numeric(10, 2),
    source_product_quantity integer,
    sale_date date NOT NULL
);

CREATE INDEX idx_fact_sales_sale_date ON fact_sales (sale_date);
CREATE INDEX idx_fact_sales_customer_key ON fact_sales (customer_key);
CREATE INDEX idx_fact_sales_seller_key ON fact_sales (seller_key);
CREATE INDEX idx_fact_sales_store_key ON fact_sales (store_key);
CREATE INDEX idx_fact_sales_supplier_key ON fact_sales (supplier_key);
CREATE INDEX idx_fact_sales_product_key ON fact_sales (product_key);