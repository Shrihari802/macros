/*
This model calculates the daily attach rate for new payment enabled accounts (KPI-022) as the ratio of New PEs to New Customers within 60 days.

It works by:
1. Getting daily New Customers from new_customers_daily.sql (KPI-001)
2. Getting daily New Payment Enabled Accounts from new_payment_enabled_accounts_daily.sql (KPI-005)
3. Calculating the attach rate as: New PEs / New Customers for each day
4. Ensuring PEs occur within 60 days of software purchase date (KPI-5 <= KPI-1 + 60)
5. Grouping by software purchase date (KPI-1 date)
6. Handling division by zero cases appropriately
7. Labeling the metric as "Attach Rate (New PEs)"
*/

{{ config(materialized='table', dist="even") }}

WITH

-- Clean daily new customers
daily_new_customers AS (
    SELECT
          solution
        , CAST(period AS DATE)                       AS period
        , value::FLOAT                               AS new_customers_count
    FROM {{ ref('new_customers_daily') }}
    WHERE metric_key     = 'New Customers'
      AND period_grain   = 'day'
      AND period         >= DATE '2020-01-01'
)

-- Daily new payment-enabled accounts
, daily_new_pe AS (
    SELECT
          solution
        , period
        , value                                      AS new_pe_count
    FROM {{ ref('new_payment_enabled_accounts_daily') }}
    WHERE metric_key     = 'New Payment Enabled Accounts'
      AND period_grain   = 'day'
      AND period         >= DATE '2020-01-01'
)

-- Calculate attach rate over 60-day window from software purchase date
, daily_attach_rate AS (
    SELECT
          'Attach Rate (New PEs)'                    AS metric_key
        , nc.solution                                AS solution
        , nc.period                                  AS software_purchase_date
        , 'day'                                      AS period_grain
        , CASE
            WHEN COALESCE(nc.new_customers_count, 0) = 0 THEN NULL
            ELSE (
                SELECT COALESCE(SUM(pe.new_pe_count), 0)
                FROM daily_new_pe pe
                WHERE pe.solution = nc.solution
                  AND pe.period BETWEEN nc.period AND nc.period + INTERVAL '60 days'
            )::FLOAT / nc.new_customers_count::FLOAT
          END                                        AS value
    FROM daily_new_customers nc
)

-- Final output
SELECT
      metric_key
    , solution
    , TO_CHAR(software_purchase_date, 'YYYY-MM-DD')::DATE AS period
    , period_grain
    , CASE
      WHEN value IS NULL THEN 0.0
      ELSE CAST(ROUND(value * 100, 2) AS NUMERIC(10, 2))
      END                                                 AS value
FROM daily_attach_rate
WHERE value IS NOT NULL
ORDER BY software_purchase_date, solution
