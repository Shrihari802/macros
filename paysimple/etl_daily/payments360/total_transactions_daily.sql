/*
This model calculates the total number of transactions per day since Jan 1 2020,
combining data from PaySimple and Service Fusion.

It works by:
1. Summing daily transactions by solution from PaySimple activity data.
2. Summing daily card transactions from Service Fusion (via STAX and TSYS gateways).
3. Combining both datasets.
4. Aggregating the total transactions by solution and date.
*/

{{ config(materialized='table', dist='even') }}

-- Step 1: Aggregate PaySimple transactions per day and solution, filtering to Service Fusion & Paysimple + partners
WITH paysimple_transactions AS (
    SELECT
          'Total Transactions'                                     AS metric_key
        , LOWER(acc.solution)                                      AS solution
        , CAST(act.transaction_date AS DATE)                       AS period
        , 'day'                                                    AS period_grain
        , SUM(act.txn)                                             AS value
    FROM {{ ref ('paysimple_production_360_activity')}} act
    LEFT JOIN {{ ref ( 'paysimple_production_360_accounts' )}} acc
        ON acc.account_id = act.account_id
    WHERE period >= DATE '2020-01-01'
      AND LOWER(acc.solution) IN (
            'service fusion'
          , 'direct: paysimple'
          , 'integrated partner: third party'
          , 'integrated partner: zen planner'
      )
    GROUP BY 1, 2, 3, 4
)

-- Step 2: Aggregate Service Fusion card transactions by day
, fusion_gateway_transactions AS (
    SELECT
          'Total Transactions'                                     AS metric_key
        , 'service fusion'                                         AS solution
        , TO_DATE(cp.year || '-' || LPAD(cp.month::VARCHAR, 2, '0') || '-' || LPAD(cp.day::VARCHAR, 2, '0'), 'YYYY-MM-DD')
                                                                  AS period
        , 'day'                                                    AS period_grain
        , SUM(cp.transaction_count)                                AS value
    FROM {{ ref ('servicefusion_cohort_payment')}} cp
    LEFT JOIN {{ ref ('servicefusion_billing')}} b
        ON b.coid = cp.coid
    WHERE b.isactive IS NOT NULL
      AND LOWER(cp.payment_type) = 'card'
      AND LOWER(cp.gateway) IN ('stax', 'tsys (formerly cayan)')
    GROUP BY 1, 2, 3, 4
)

-- Step 3: Combine both datasets
, combined_transactions AS (
    SELECT * FROM paysimple_transactions
    UNION ALL
    SELECT * FROM fusion_gateway_transactions
)

, final_aggregated AS (
    SELECT
          metric_key
        , LOWER(solution)                                          AS solution 
        , period::DATE
        , period_grain
        , SUM(value)::DECIMAL                                      AS value
    FROM combined_transactions
    GROUP BY 1, 2, 3, 4
)

-- Step 4: Aggregate total transactions per day and solution
SELECT *
FROM final_aggregated
ORDER BY period, solution
