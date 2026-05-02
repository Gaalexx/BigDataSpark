CREATE DATABASE IF NOT EXISTS lab2chs;

USE lab2chs;

CREATE TABLE IF NOT EXISTS report_product_sales (
    product_name Nullable(String),
    product_category Nullable(String),
    pet_category Nullable(String),
    brand Nullable(String),
    color Nullable(String),
    size Nullable(String),
    material Nullable(String),
    unit_price Nullable(Float64),
    avg_rating Nullable(Float64),
    reviews_count Nullable(Int32),
    total_revenue Float64,
    total_quantity_sold Int64,
    total_sales_count Int64,
    revenue_rank Int32,
    quantity_rank Int32
)
ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS report_customer_sales (
    customer_email Nullable(String),
    first_name Nullable(String),
    last_name Nullable(String),
    country Nullable(String),
    postal_code Nullable(String),
    pet_type Nullable(String),
    pet_breed Nullable(String),
    total_purchase_amount Float64,
    total_orders_count Int64,
    total_quantity_purchased Int64,
    avg_check Float64,
    spend_rank Int32
)
ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS report_time_sales (
    sale_year Int32,
    sale_month Int32,
    month_start Date,
    total_revenue Float64,
    total_orders_count Int64,
    total_quantity_sold Int64,
    avg_order_size Float64,
    prev_month_revenue Nullable(Float64),
    prev_year_same_month_revenue Nullable(Float64),
    revenue_change_vs_prev_month Nullable(Float64),
    revenue_change_vs_prev_year_same_month Nullable(Float64),
    year_total_revenue Float64
)
ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS report_store_sales (
    store_email Nullable(String),
    store_name Nullable(String),
    location Nullable(String),
    city Nullable(String),
    state Nullable(String),
    country Nullable(String),
    total_revenue Float64,
    total_orders_count Int64,
    total_quantity_sold Int64,
    avg_check Float64,
    revenue_rank Int32
)
ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS report_supplier_sales (
    supplier_email Nullable(String),
    supplier_name Nullable(String),
    contact_name Nullable(String),
    city Nullable(String),
    country Nullable(String),
    avg_product_price Nullable(Float64),
    total_revenue Float64,
    total_orders_count Int64,
    total_quantity_sold Int64,
    revenue_rank Int32
)
ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS report_product_quality (
    product_name Nullable(String),
    product_category Nullable(String),
    pet_category Nullable(String),
    brand Nullable(String),
    color Nullable(String),
    size Nullable(String),
    material Nullable(String),
    rating Nullable(Float64),
    reviews_count Nullable(Int32),
    total_quantity_sold Int64,
    total_revenue Float64,
    rating_rank_desc Int32,
    rating_rank_asc Int32,
    reviews_rank Int32,
    sales_rank Int32,
    rating_sales_correlation Nullable(Float64)
)
ENGINE = MergeTree()
ORDER BY tuple();
