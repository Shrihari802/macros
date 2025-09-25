/*
This model calculates the daily total processing volume (TPV) for payment-enabled accounts
since Jan 1 2020, combining data from PaySimple and Service Fusion.

It works by:
1. Summing TPV by day and solution from PaySimple transaction activity.
2. Summing TPV by day for Service Fusion card payments (via STAX and TSYS gateways).
3. Combining both datasets.
4. Filtering to Paysimple + partners & Service Fusion.
5. Aggregating total TPV per solution and day.
6. Labeling the metric as "Total Processing Volume".
*/

{{ config(materialized='table', dist="even") }}

-- Step 1: Aggregate PaySimple TPV per day and solution
WITH ps_tpv AS (
    SELECT
          'Total Processing Volume'                                                  AS metric_name
        , LOWER(acc.solution)                                                        AS solution
        , CAST(act.transaction_date AS DATE)                                         AS period
        , 'day'                                                                      AS period_grain
        , SUM(act.tpv)                                                               AS value
    FROM {{ ref ('paysimple_production_360_activity')}} act
    LEFT JOIN {{ ref ('paysimple_production_360_accounts')}} acc
        ON acc.account_id = act.account_id
    WHERE period >= DATE '2020-01-01'
    GROUP BY 1, 2, 3, 4
)

-- Step 2: Aggregate Service Fusion TPV for card payments via STAX and TSYS gateways
, sf_stax_tsys_tpv AS (
    SELECT
          'Total Processing Volume'                                                  AS metric_name
        , 'service fusion'                                                           AS solution
        , TO_DATE(cp.year || '-' || LPAD(cp.month::VARCHAR, 2, '0') || '-' || LPAD(cp.day::VARCHAR, 2, '0'), 'YYYY-MM-DD') AS period
        , 'day'                                                                      AS period_grain
        , SUM(cp.tpv)                                                                AS value
    FROM {{ ref ('servicefusion_cohort_payment')}} cp
    WHERE LOWER(cp.payment_type) = 'card'
    AND LOWER(cp.gateway) IN ('stax', 'tsys (formerly cayan)')
    AND cp.year >= '2020'
    GROUP BY 1, 2, 3, 4
)

-- Step 3: Combine both sources of TPV data
, combined_tpv AS (
    SELECT * FROM ps_tpv
    UNION ALL
    SELECT * FROM sf_stax_tsys_tpv
)

-- Step 4: Filter to relevant solutions and aggregate final TPV per day
SELECT
      metric_name                                                                    AS metric_key
    , LOWER(solution)                                                                AS solution
    , period::DATE
    , period_grain
    , SUM(value)::DECIMAL                                                            AS value
FROM combined_tpv
WHERE LOWER(solution) IN (
      'service fusion'
    , 'direct: paysimple'
    , 'integrated partner: third party'
    , 'integrated partner: zen planner'
)
GROUP BY 1, 2, 3, 4
ORDER BY period, solution
