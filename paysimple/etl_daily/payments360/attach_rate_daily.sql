/*
This model calculates the daily attach rate (KPI-023) as the ratio of New Payment Enabled Accounts to New Customers.

It works by:
1. Getting daily New Payment Enabled Accounts from new_payment_enabled_accounts_daily.sql
2. Getting daily New Customers from new_customers_daily.sql
3. Calculating the attach rate as: New PEs / New Customers for each day
4. Handling division by zero cases appropriately
5. Labeling the metric as "Attach Rate (ALL PEs)"
*/

{{ config(materialized='table', dist="even") }}

WITH

-- Getting daily New Payment Enabled Accounts (KPI-005)
daily_new_pe AS (
    SELECT
          solution
        , period
        , value                                        AS new_pe_count
    FROM {{ ref('new_payment_enabled_accounts_daily') }}
)

-- Getting daily New Customers (KPI-001)
, daily_new_customers AS (
    SELECT
          solution
        , period
        , value                                        AS new_customers_count
    FROM {{ ref('new_customers_daily') }}
)

-- Calculating daily attach rate
, daily_attach_rate AS (
    SELECT
          'Attach Rate (ALL PEs)'                      AS metric_key
        , pe.solution                                  AS solution
        , pe.period                                    AS period
        , 'day'                                        AS period_grain
        , CASE
            WHEN COALESCE(nc.new_customers_count, 0) = 0 THEN NULL
            ELSE COALESCE(pe.new_pe_count, 0)::DECIMAL / nc.new_customers_count::DECIMAL
          END                                          AS value
    FROM daily_new_pe pe
    FULL OUTER JOIN daily_new_customers nc
      ON pe.solution = nc.solution
     AND pe.period   = nc.period
)

-- Final output
SELECT
      metric_key
    , solution
    , period::DATE
    , period_grain
    , CASE
        WHEN value IS NULL THEN 0.0
        ELSE CAST(ROUND(value * 100, 2) AS NUMERIC(10, 2))
      END                                             AS value
FROM daily_attach_rate
WHERE value IS NOT NULL
ORDER BY period, solution
