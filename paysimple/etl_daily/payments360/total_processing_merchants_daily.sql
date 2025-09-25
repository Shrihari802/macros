/*
This model calculates the daily count of total processing merchants (clients with successful 
credit card transactions) starting from January 1, 2020, combining data from PaySimple and Service Fusion.

It works by:
1. Filtering PaySimple transactions for relevant solutions, where payment method = 'CC' (credit card), and has positive successful counts.
2. Aggregating distinct clients per day and solution from PaySimple data.
3. Selecting Service Fusion transactions with card payments and active billing accounts.
4. Aggregating distinct Service Fusion clients per day.
5. Combining both data sources and summing counts by solution and day.
6. Labeling the metric as "Total Processing Merchants".
*/


{{ config(materialized='table', dist='even') }}

-- Step 1: Extract PaySimple transactions meeting criteria
WITH base_data AS (
    SELECT
          t.client_id
        , LOWER(a.finance_bucket)                                    AS account_finance_bucket
        , DATE_TRUNC('day', t.financial_date)                            AS transaction_day
    FROM dbt_everpro.paysimple_transactions t
    LEFT JOIN dbt_everpro.paysimple_salesforce_account a
        ON t.account_id = a.id
    WHERE t.financial_date >= TIMESTAMP '2020-01-01'
      AND LOWER(a.finance_bucket) IN (
    'service fusion',
    'direct: paysimple',
    'integrated partner: third party',
    'integrated partner: zen planner'
)
      AND t.payment_method = 'CC'
    GROUP BY 1, 2, 3
    HAVING SUM(t.cc_financial_successful_count) > 0
)

-- Step 2: Aggregate unique clients by solution and day from PaySimple data
, paysimple_rollup AS (
    SELECT
          LOWER(account_finance_bucket)                              AS solution
        , transaction_day                                            AS period
        , COUNT(DISTINCT client_id)                                  AS value
    FROM base_data
    GROUP BY 1, 2
)

-- Step 3: Extract Service Fusion card payment transactions with active billing
, fusion_gateway_txns AS (
    SELECT
          TO_DATE(cp.year || '-' || LPAD(cp.month::VARCHAR, 2, '0') || '-' || LPAD(cp.day::VARCHAR, 2, '0'), 'YYYY-MM-DD') AS transaction_day
        , cp.coid                                                    AS client_id
    FROM dbt_everpro.servicefusion_cohort_payment cp
    LEFT JOIN dbt_everpro.servicefusion_billing b
        ON b.coid = cp.coid
    WHERE LOWER(cp.gateway) IN ('stax', 'tsys (formerly cayan)')
      AND cp.payment_type = 'CARD'
      AND b.isactive IS NOT NULL
      AND cp.year >= 2020
    GROUP BY 1, 2
)

-- Step 4: Aggregate unique Service Fusion clients per day
, fusion_gateway_agg AS (
    SELECT
          'service fusion'                                           AS solution
        , transaction_day                                            AS period
        , COUNT(DISTINCT client_id)                                  AS value
    FROM fusion_gateway_txns
    GROUP BY 1, 2
)

-- Step 5: Combine both PaySimple and Service Fusion counts and sum per solution and day
, combined_rollup AS (
    SELECT
          solution
        , period::DATE
        , SUM(value)                                                 AS value
    FROM (
        SELECT * FROM paysimple_rollup
        UNION ALL
        SELECT * FROM fusion_gateway_agg
    ) all_data
    GROUP BY solution, period
)

-- Step 6: Final output
SELECT
      'Total Processing Merchants'                                   AS metric_key
    , solution
    , period::DATE
    , 'day'                                                          AS period_grain
    , value::DECIMAL
FROM combined_rollup
ORDER BY period, solution
