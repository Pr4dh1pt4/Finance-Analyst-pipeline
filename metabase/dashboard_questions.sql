-- =====================================================================
-- METABASE QUESTION PACK — DustiniaDelixia Finance Dashboard
-- =====================================================================
-- HOW TO USE:
--   1. In Metabase, add a database: type = ClickHouse,
--      host = clickhouse, port = 8123, db = dustinia_marts, user = default.
--      (Install the ClickHouse driver .jar into /plugins if not bundled.)
--   2. For each block below: New > SQL question > paste > Visualize.
--   3. Save all into a dashboard called "Finance: High-Value Customers".
-- Each query is self-contained and reads only from the marts layer.
-- =====================================================================


-- ---------------------------------------------------------------------
-- CARD 1 — KPI: total HVC revenue & share (Big Number / Scalar)
-- ---------------------------------------------------------------------
SELECT
    round(sumIf(monetary, is_high_value_customer))                       AS hvc_revenue,
    round(sumIf(monetary, is_high_value_customer) / sum(monetary) * 100, 1) AS hvc_revenue_share_pct,
    countIf(is_high_value_customer)                                      AS hvc_count
FROM dustinia_marts.dim_customer;


-- ---------------------------------------------------------------------
-- CARD 2 — AOV by value segment (Bar chart)
-- Shows the ~5x spend gap between HVCs and everyone else.
-- ---------------------------------------------------------------------
SELECT
    value_segment,
    round(avg(avg_order_value), 2) AS avg_order_value,
    count()                        AS customers
FROM dustinia_marts.dim_customer
GROUP BY value_segment
ORDER BY avg_order_value DESC;


-- ---------------------------------------------------------------------
-- CARD 3 — Payment method mix within HVCs (Pie / Row chart)
-- "What do high-value customers actually pay with?"
-- ---------------------------------------------------------------------
SELECT
    primary_payment_type,
    sum(order_count)         AS orders,
    round(sum(total_revenue))AS revenue
FROM dustinia_marts.mart_hvc_payment_profile
WHERE value_segment IN ('Top 5% Whale', 'High Value')
GROUP BY primary_payment_type
ORDER BY revenue DESC;


-- ---------------------------------------------------------------------
-- CARD 4 — Installment behaviour by segment (Combo: bar + line)
-- The core insight: higher-value customers lean on installments more,
-- and installment orders carry a higher AOV.
-- ---------------------------------------------------------------------
SELECT
    value_segment,
    round(avg(pct_orders_with_installments), 1) AS pct_orders_with_installments,
    round(avg(avg_installments_when_used), 2)   AS avg_installments_when_used
FROM dustinia_marts.mart_hvc_payment_profile
GROUP BY value_segment
ORDER BY pct_orders_with_installments DESC;


-- ---------------------------------------------------------------------
-- CARD 5 — Geographic HVC concentration (Map / Region map on BR states)
-- Set "Display = Map", region = Brazil states, metric = hvc_revenue.
-- ---------------------------------------------------------------------
SELECT
    customer_state,
    total_customers,
    hvc_customers,
    hvc_penetration_pct,
    round(hvc_revenue)     AS hvc_revenue,
    hvc_revenue_share_pct
FROM dustinia_marts.mart_geo_value
ORDER BY hvc_revenue DESC;


-- ---------------------------------------------------------------------
-- CARD 6 — THE MONEY SLIDE: payment-optimisation uplift (Bar chart)
-- Revenue currently left on the table because credit-card orders are
-- NOT using installments. Projected at a conservative 30% adoption.
-- ---------------------------------------------------------------------
SELECT
    value_segment,
    aov_no_installment,
    aov_installment,
    aov_gap,
    addressable_orders,
    round(projected_annual_uplift) AS projected_uplift
FROM dustinia_marts.mart_payment_uplift
ORDER BY projected_uplift DESC;


-- ---------------------------------------------------------------------
-- CARD 7 — HVC revenue trend over time (Line chart)
-- Monthly HVC revenue to show momentum / seasonality.
-- ---------------------------------------------------------------------
SELECT
    f.order_month,
    round(sumIf(f.order_value, d.is_high_value_customer)) AS hvc_revenue,
    round(sumIf(f.order_value, NOT d.is_high_value_customer)) AS other_revenue
FROM dustinia_marts.fct_order_payments f
INNER JOIN dustinia_marts.dim_customer d USING (customer_unique_id)
WHERE f.order_month IS NOT NULL
GROUP BY f.order_month
ORDER BY f.order_month;


-- ---------------------------------------------------------------------
-- CARD 8 — Top HVC cities (Table, with a dashboard filter on state)
-- ---------------------------------------------------------------------
SELECT
    customer_state,
    customer_city,
    countIf(is_high_value_customer)             AS hvc_customers,
    round(sumIf(monetary, is_high_value_customer)) AS hvc_revenue,
    round(avgIf(avg_order_value, is_high_value_customer), 2) AS hvc_avg_order
FROM dustinia_marts.dim_customer
WHERE is_high_value_customer
GROUP BY customer_state, customer_city
ORDER BY hvc_revenue DESC
LIMIT 25;
