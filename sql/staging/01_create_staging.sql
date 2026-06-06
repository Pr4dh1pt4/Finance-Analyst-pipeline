-- =====================================================================
-- DustiniaDelixia Groceria — Finance Analyst Pipeline
-- Layer 1: STAGING (raw landing zone)
-- Engine: ClickHouse 24.x
-- These tables mirror the source CSVs 1:1. No business logic here.
-- =====================================================================

CREATE DATABASE IF NOT EXISTS dustinia_raw;
CREATE DATABASE IF NOT EXISTS dustinia_marts;

-- ---------------------------------------------------------------------
-- orders
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS dustinia_raw.orders;
CREATE TABLE dustinia_raw.orders
(
    order_id                       String,
    customer_id                    String,
    order_status                   LowCardinality(String),
    order_purchase_timestamp       Nullable(DateTime),
    order_approved_at              Nullable(DateTime),
    order_delivered_carrier_date   Nullable(DateTime),
    order_delivered_customer_date  Nullable(DateTime),
    order_estimated_delivery_date  Nullable(DateTime)
)
ENGINE = MergeTree
ORDER BY (order_id);

-- ---------------------------------------------------------------------
-- order_payments
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS dustinia_raw.order_payments;
CREATE TABLE dustinia_raw.order_payments
(
    order_id              String,
    payment_sequential    UInt16,
    payment_type          LowCardinality(String),
    payment_installments  UInt8,
    payment_value         Decimal(12, 2)
)
ENGINE = MergeTree
ORDER BY (order_id, payment_sequential);

-- ---------------------------------------------------------------------
-- order_items
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS dustinia_raw.order_items;
CREATE TABLE dustinia_raw.order_items
(
    order_id            String,
    order_item_id       UInt16,
    product_id          String,
    seller_id           String,
    shipping_limit_date Nullable(DateTime),
    price               Decimal(12, 2),
    freight_value       Decimal(12, 2)
)
ENGINE = MergeTree
ORDER BY (order_id, order_item_id);

-- ---------------------------------------------------------------------
-- customers
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS dustinia_raw.customers;
CREATE TABLE dustinia_raw.customers
(
    customer_id              String,
    customer_unique_id       String,
    customer_zip_code_prefix String,
    customer_city            String,
    customer_state           LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (customer_id);

-- ---------------------------------------------------------------------
-- geolocation  (deduplicated to one lat/lng per zip prefix downstream)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS dustinia_raw.geolocation;
CREATE TABLE dustinia_raw.geolocation
(
    geolocation_zip_code_prefix String,
    geolocation_lat             Float64,
    geolocation_lng             Float64,
    geolocation_city            String,
    geolocation_state           LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (geolocation_zip_code_prefix);

-- ---------------------------------------------------------------------
-- order_reviews (used as a secondary signal: do HVCs review differently?)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS dustinia_raw.order_reviews;
CREATE TABLE dustinia_raw.order_reviews
(
    review_id               String,
    order_id                String,
    review_score            UInt8,
    review_comment_title    Nullable(String),
    review_comment_message  Nullable(String),
    review_creation_date    Nullable(DateTime),
    review_answer_timestamp Nullable(DateTime)
)
ENGINE = MergeTree
ORDER BY (order_id);
