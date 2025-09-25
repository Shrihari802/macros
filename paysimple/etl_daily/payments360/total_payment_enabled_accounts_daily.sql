/*
This model calculates the total number of payment-enabled accounts active each day 
since jan 1 2020, combining data from PaySimple and Service Fusion.

It works by:
1. Generating a sequence of dates from jan 1 2020 onward.
2. Selecting enabled accounts from PaySimple with lifecycle dates.
3. Selecting enabled accounts from Service Fusion with specific payment gateways.
4. Combining accounts from both sources.
5. Counting the distinct active accounts per day, considering enable and exit dates.
6. Labeling the metric as "Total Payment Enabled Accounts".
*/

{{ config(materialized='table', dist='even') }}

-- Generate dates from jan 1 2020 onward
WITH RECURSIVE day_sequence(period) AS (
    SELECT DATE '2020-01-01' AS period
    UNION ALL
    SELECT CAST(period + INTERVAL '1 day' AS DATE)
    FROM day_sequence
    WHERE period + INTERVAL '1 day' <= CURRENT_DATE
)

-- PaySimple enabled accounts with lifecycle tracking
, paysimple_accounts AS (
    SELECT
          account_id
        , LOWER(solution)                        AS solution 
        , CAST(payments_enable_date AS DATE)     AS enable_date
        , CAST(payments_exit_date AS DATE)       AS exit_date
    FROM {{ ref( 'paysimple_production_360_accounts') }}
    WHERE payments_enable_date IS NOT NULL
    AND payments_enable_date >= '01-01-2020'
)

-- Service Fusion accounts with STAX or TSYS gateway
, fusion_accounts AS (
    SELECT
          CAST(coid AS VARCHAR)                  AS account_id
        , 'service fusion'                       AS solution
        , CAST(payment_enable_date AS DATE)      AS enable_date
        , CAST(exit_software_date AS DATE)       AS exit_date
    FROM {{ ref( 'servicefusion_production_360_accounts') }}
    WHERE payment_enable_date IS NOT NULL
    AND payment_enable_date >= '01-01-2020'
    AND (LOWER(gateway) = 'stax' OR LOWER(gateway) = 'tsys (formerly cayan)')
)

-- Combine both sources
, enabled_accounts_union AS (
    SELECT * FROM paysimple_accounts
    UNION ALL
    SELECT * FROM fusion_accounts
)

-- Count active accounts for each day
SELECT
      'Total Payment Enabled Accounts'           AS metric_key
    , ea.solution
    , ds.period::DATE                            AS period
    , 'day'                                      AS period_grain
    , COUNT(DISTINCT ea.account_id)::DECIMAL     AS value
FROM day_sequence ds
JOIN enabled_accounts_union ea
  ON ea.enable_date <= ds.period
 AND (ea.exit_date IS NULL OR ea.exit_date > ds.period)
GROUP BY 
      metric_key
    , ea.solution
    , ds.period
    , period_grain
ORDER BY period, solution
