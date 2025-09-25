/*
This model calculates the daily activation rate (KPI-025) as the ratio of New Processing Merchants to New Payment Enabled Accounts.

It works by:
1. Getting daily New Processing Merchants from new_processing_merchants_daily.sql (KPI-009)
2. Getting daily New Payment Enabled Accounts from new_payment_enabled_accounts_daily.sql (KPI-005)
3. Calculating the activation rate as: New Processing Merchants / New PEs for each day
4. Ensuring activations occur within 60 days of PE date (KPI-9 <= KPI-5 + 60)
5. Handling division by zero cases appropriately
6. Labeling the metric as "Activation Rate"
*/

{{ config(materialized='table', dist="even") }}

WITH

-- Getting daily New Processing Merchants (KPI-009)
daily_new_processing_merchants AS (
    SELECT
          solution
        , period
        , value                                       AS new_processing_merchants_count
    FROM {{ ref('new_processing_merchants_daily') }}
    WHERE metric_key   = 'New Processing Merchants'
      AND period_grain = 'day'
      AND period       >= DATE '2020-01-01'
)

-- Getting daily New Payment Enabled Accounts (KPI-005)
, daily_new_pe AS (
    SELECT
          solution
        , period
        , value                                       AS new_pe_count
    FROM {{ ref('new_payment_enabled_accounts_daily') }}
    WHERE metric_key   = 'New Payment Enabled Accounts'
      AND period_grain = 'day'
      AND period       >= DATE '2020-01-01'
)

-- Calculating daily activation rate with 60-day window logic
, daily_activation_rate AS (
    SELECT
          'Activation Rate'                           AS metric_key
        , COALESCE(pm.solution, pe.solution)          AS solution
        , COALESCE(pm.period, pe.period)              AS period
        , 'day'                                       AS period_grain
        , CASE
            WHEN COALESCE(pe.new_pe_count, 0) = 0 THEN NULL
            WHEN pm.period <= pe.period + INTERVAL '60 days'
                THEN COALESCE(pm.new_processing_merchants_count, 0)::FLOAT / pe.new_pe_count
            ELSE 0
          END                                         AS value
    FROM daily_new_processing_merchants pm
    FULL OUTER JOIN daily_new_pe pe
      ON pm.solution = pe.solution
     AND pm.period   = pe.period
)

-- Final output
SELECT
      metric_key
    , solution
    , TO_CHAR(period, 'YYYY-MM-DD')::DATE                   AS period
    , period_grain
    , CASE
        WHEN value IS NULL THEN NULL
        ELSE CAST(ROUND(value * 100, 2) AS NUMERIC(10, 2))
      END                                                   AS value
FROM daily_activation_rate
WHERE value IS NOT NULL
ORDER BY period, solution
