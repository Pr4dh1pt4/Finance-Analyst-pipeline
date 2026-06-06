-- =====================================================================
-- DustiniaDelixia Groceria — Finance Analyst Pipeline
-- Layer 2: MARTS (dimensional model + analytical aggregates)
-- All objects rebuilt idempotently each DAG run.
-- =====================================================================

-- =====================================================================
-- dim_geolocation : one representative coordinate per zip prefix
-- (geolocation.csv has ~1M rows with many points per prefix)
-- =====================================================================
DROP TABLE IF EXISTS dustinia_marts.dim_geolocation;
CREATE TABLE dustinia_marts.dim_geolocation
ENGINE = MergeTree ORDER BY (zip_code_prefix)
AS
SELECT
    geolocation_zip_code_prefix                 AS zip_code_prefix,
    avg(geolocation_lat)                        AS lat,
    avg(geolocation_lng)                        AS lng,
    anyHeavy(geolocation_city)                  AS city,
    anyHeavy(geolocation_state)                 AS state
FROM dustinia_raw.geolocation
GROUP BY geolocation_zip_code_prefix;

-- =====================================================================
-- fct_order_payments : one row per order with payment behaviour rolled up
-- This is the grain the Finance persona reasons about: the order + how
-- it was paid for + where the customer is.
-- =====================================================================
DROP TABLE IF EXISTS dustinia_marts.fct_order_payments;
CREATE TABLE dustinia_marts.fct_order_payments
ENGINE = MergeTree ORDER BY (order_id)
AS
WITH
-- payment rolled up to order level
pay AS (
    SELECT
        order_id,
        sum(payment_value)                                          AS order_value,
        max(payment_installments)                                   AS max_installments,
        countDistinct(payment_type)                                 AS distinct_payment_methods,
        -- the "primary" payment method = the one carrying the most money
        argMax(payment_type, payment_value)                         AS primary_payment_type,
        sumIf(payment_value, payment_type = 'credit_card')          AS credit_card_value,
        sumIf(payment_value, payment_type = 'boleto')               AS boleto_value,
        sumIf(payment_value, payment_type = 'voucher')              AS voucher_value,
        sumIf(payment_value, payment_type = 'debit_card')           AS debit_card_value,
        max(payment_installments) > 1                               AS used_installments
    FROM dustinia_raw.order_payments
    GROUP BY order_id
),
-- item value rolled up to order level (product price vs freight split)
itm AS (
    SELECT
        order_id,
        sum(price)          AS items_price,
        sum(freight_value)  AS freight_value,
        count()             AS item_count
    FROM dustinia_raw.order_items
    GROUP BY order_id
)
SELECT
    o.order_id                                          AS order_id,
    o.customer_id                                       AS customer_id,
    c.customer_unique_id                                AS customer_unique_id,
    o.order_status                                      AS order_status,
    o.order_purchase_timestamp                          AS order_purchase_timestamp,
    toDate(o.order_purchase_timestamp)                  AS order_date,
    toStartOfMonth(o.order_purchase_timestamp)          AS order_month,
    c.customer_state                                    AS customer_state,
    c.customer_city                                     AS customer_city,
    c.customer_zip_code_prefix                          AS customer_zip_code_prefix,
    p.order_value                                       AS order_value,
    p.primary_payment_type                              AS primary_payment_type,
    p.max_installments                                  AS max_installments,
    p.used_installments                                 AS used_installments,
    p.distinct_payment_methods                          AS distinct_payment_methods,
    p.credit_card_value                                 AS credit_card_value,
    p.boleto_value                                      AS boleto_value,
    p.voucher_value                                     AS voucher_value,
    p.debit_card_value                                  AS debit_card_value,
    i.items_price                                       AS items_price,
    i.freight_value                                     AS freight_value,
    i.item_count                                        AS item_count
FROM dustinia_raw.orders o
INNER JOIN pay p USING (order_id)
LEFT  JOIN itm i USING (order_id)
LEFT  JOIN dustinia_raw.customers c ON o.customer_id = c.customer_id
WHERE o.order_status != 'canceled';

-- =====================================================================
-- dim_customer : customer-level aggregation with RFM + HVC flag
-- High-Value Customer (HVC) is defined at the customer level by total
-- lifetime spend, then flagged at the top decile.
-- =====================================================================
DROP TABLE IF EXISTS dustinia_marts.dim_customer;
CREATE TABLE dustinia_marts.dim_customer
ENGINE = MergeTree ORDER BY (customer_unique_id)
AS
WITH base AS (
    SELECT
        customer_unique_id,
        any(customer_state)                          AS customer_state,
        any(customer_city)                           AS customer_city,
        count()                                      AS frequency,
        sum(order_value)                             AS monetary,
        avg(order_value)                             AS avg_order_value,
        max(order_date)                              AS last_order_date,
        avgIf(max_installments, used_installments)   AS avg_installments_when_used,
        countIf(used_installments) / count()         AS installment_order_share,
        argMax(primary_payment_type, order_value)    AS top_payment_type
    FROM dustinia_marts.fct_order_payments
    WHERE customer_unique_id != ''
    GROUP BY customer_unique_id
),
scored AS (
    SELECT
        *,
        dateDiff('day', last_order_date, (SELECT max(order_date) FROM dustinia_marts.fct_order_payments)) AS recency_days,
        -- monetary quantile rank across whole base
        quantileExact(0.90)(monetary) OVER ()        AS p90_monetary,
        quantileExact(0.95)(monetary) OVER ()        AS p95_monetary
    FROM base
)
SELECT
    customer_unique_id,
    customer_state,
    customer_city,
    frequency,
    monetary,
    avg_order_value,
    recency_days,
    last_order_date,
    avg_installments_when_used,
    installment_order_share,
    top_payment_type,
    monetary >= p95_monetary                          AS is_top5_value,
    monetary >= p90_monetary                          AS is_high_value_customer,
    multiIf(
        monetary >= p95_monetary, 'Top 5% Whale',
        monetary >= p90_monetary, 'High Value',
        monetary >= (SELECT median(monetary) FROM base), 'Mid Value',
        'Standard'
    )                                                 AS value_segment
FROM scored;

-- =====================================================================
-- mart_hvc_payment_profile : the headline answer for the Head of Finance
-- "Who are HVCs, how do they pay, where are they?"
-- =====================================================================
DROP TABLE IF EXISTS dustinia_marts.mart_hvc_payment_profile;
CREATE TABLE dustinia_marts.mart_hvc_payment_profile
ENGINE = MergeTree ORDER BY (value_segment, primary_payment_type)
AS
SELECT
    d.value_segment                                   AS value_segment,
    f.primary_payment_type                            AS primary_payment_type,
    count()                                           AS order_count,
    countDistinct(f.customer_unique_id)               AS customer_count,
    round(sum(f.order_value), 2)                      AS total_revenue,
    round(avg(f.order_value), 2)                      AS avg_order_value,
    round(avg(f.max_installments), 2)                 AS avg_installments,
    round(avgIf(f.max_installments, f.used_installments), 2) AS avg_installments_when_used,
    round(countIf(f.used_installments) / count() * 100, 1)   AS pct_orders_with_installments
FROM dustinia_marts.fct_order_payments f
INNER JOIN dustinia_marts.dim_customer d USING (customer_unique_id)
GROUP BY value_segment, primary_payment_type;

-- =====================================================================
-- mart_geo_value : HVC concentration by state (feeds the Metabase map)
-- =====================================================================
DROP TABLE IF EXISTS dustinia_marts.mart_geo_value;
CREATE TABLE dustinia_marts.mart_geo_value
ENGINE = MergeTree ORDER BY (customer_state)
AS
SELECT
    d.customer_state                                  AS customer_state,
    g.lat                                             AS lat,
    g.lng                                             AS lng,
    countDistinct(d.customer_unique_id)               AS total_customers,
    countDistinctIf(d.customer_unique_id, d.is_high_value_customer) AS hvc_customers,
    round(countDistinctIf(d.customer_unique_id, d.is_high_value_customer)
          / countDistinct(d.customer_unique_id) * 100, 2)          AS hvc_penetration_pct,
    round(sum(d.monetary), 2)                         AS total_revenue,
    round(sumIf(d.monetary, d.is_high_value_customer), 2)          AS hvc_revenue,
    round(sumIf(d.monetary, d.is_high_value_customer)
          / sum(d.monetary) * 100, 1)                 AS hvc_revenue_share_pct
FROM dustinia_marts.dim_customer d
LEFT JOIN (
    SELECT state, avg(lat) AS lat, avg(lng) AS lng
    FROM dustinia_marts.dim_geolocation GROUP BY state
) g ON d.customer_state = g.state
GROUP BY d.customer_state, g.lat, g.lng;

-- =====================================================================
-- mart_payment_uplift : the VALUE-ADD model.
-- Quantifies the revenue Finance is leaving on the table by NOT offering
-- installments. For each segment we compare AOV of installment orders vs
-- non-installment orders, and project the uplift if non-installment
-- credit-card orders converted at the installment AOV.
-- =====================================================================
DROP TABLE IF EXISTS dustinia_marts.mart_payment_uplift;
CREATE TABLE dustinia_marts.mart_payment_uplift
ENGINE = MergeTree ORDER BY (value_segment)
AS
WITH seg AS (
    SELECT
        d.value_segment                               AS value_segment,
        avgIf(f.order_value, f.used_installments)     AS aov_installment,
        avgIf(f.order_value, NOT f.used_installments) AS aov_no_installment,
        countIf(NOT f.used_installments
                AND f.primary_payment_type = 'credit_card') AS addressable_orders,
        sumIf(f.order_value, NOT f.used_installments
                AND f.primary_payment_type = 'credit_card') AS addressable_revenue
    FROM dustinia_marts.fct_order_payments f
    INNER JOIN dustinia_marts.dim_customer d USING (customer_unique_id)
    GROUP BY d.value_segment
)
SELECT
    value_segment,
    round(aov_installment, 2)                         AS aov_installment,
    round(aov_no_installment, 2)                      AS aov_no_installment,
    round(aov_installment - aov_no_installment, 2)    AS aov_gap,
    addressable_orders,
    round(addressable_revenue, 2)                     AS current_addressable_revenue,
    -- conservative projection: assume 30% of addressable orders adopt
    -- installments and lift to the installment AOV
    round(addressable_orders * 0.30
          * greatest(aov_installment - aov_no_installment, 0), 2) AS projected_annual_uplift
FROM seg;
