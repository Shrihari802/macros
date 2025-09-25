/*
This model calculates the daily number of new payment-enabled accounts since January 1 2020 for PaySimple and Service Fusion.

It works by:
1. Counting new accounts from PaySimple with non-null payments_enable_date.
2. Counting new accounts from Service Fusion with valid payment_enable_date & 
   using specific gateways ('STAX', 'TSYS').
3. Combining both datasets and aggregating the daily totals by solution and date.
4. Labeling the metric as "New Payment Enabled Accounts".
*/

{{ config(materialized='table', dist='even') }}

WITH
-- PaySimple new payment enabled accounts (daily)
paysimple_enabled_accounts AS (
    SELECT
          'New Payment Enabled Accounts'              AS metric_key
        , LOWER(solution)                             AS solution 
        , CAST(payments_enable_date AS DATE)          AS period
        , 'day'                                       AS period_grain
        , COUNT(*)                                    AS value
    FROM {{ ref('paysimple_production_360_accounts') }}
    WHERE payments_enable_date IS NOT NULL
      AND payments_enable_date::DATE >= DATE '2020-01-01'
      AND LOWER(solution) IN (   
          'service fusion'
        , 'direct: paysimple'
        , 'integrated partner: third party'
        , 'integrated partner: zen planner')
    GROUP BY 1, 2, 3, 4
),

-- Service Fusion payment-enabled accounts with STAX or TSYS (daily)
fusion_enabled_accounts AS (
    SELECT
          'New Payment Enabled Accounts'              AS metric_key
        , 'service fusion'                            AS solution
        , CAST(payment_enable_date AS DATE)           AS period
        , 'day'                                       AS period_grain
        , COUNT(*)                                    AS value
    FROM {{ ref('servicefusion_production_360_accounts') }}
    WHERE
          payment_enable_date IS NOT NULL
      AND LOWER(gateway) IN ('stax', 'tsys (formerly cayan)')
      AND payment_enable_date::DATE >= DATE '2020-01-01'
    GROUP BY 1, 2, 3, 4
),

-- Combine and aggregate
combined_enabled_accounts AS (
    SELECT * FROM paysimple_enabled_accounts
    UNION ALL
    SELECT * FROM fusion_enabled_accounts
)

-- Final output, ordering by day and solution
SELECT
      metric_key
    , solution
    , period::DATE
    , period_grain
    , SUM(value)::DECIMAL AS value
FROM combined_enabled_accounts
GROUP BY 1, 2, 3, 4
ORDER BY period, solution
