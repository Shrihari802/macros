/*
 This model calculates the average number of transactions per processing merchant on a daily basis,
 defined as the ratio of total transactions to the number of processing merchants.

 It works by:
 1. Pulling daily transaction counts from total_transactions_daily.
 2. Pulling daily processing merchant counts from total_processing_merchants_daily.
 3. Joining the two datasets on solution and date.
 4. Calculating the transactions per merchant as (transactions / merchants), with nulls for divide-by-zero cases.
 5. Formatting the output with consistent column structure and value rounding.
*/

{{ config(materialized='table', dist='even') }}

-- Daily transaction count per solution
WITH transactions AS (
    SELECT
          solution
        , period
        , value
    FROM {{ ref('total_transactions_daily') }}
)

-- Daily count of processing merchants per solution
, processing_merchants AS (
    SELECT
          solution
        , period
        , value
    FROM {{ ref('total_processing_merchants_daily') }}
)

-- Calculate transactions per processing merchant
SELECT
      'Transactions per Processing Merchant'         AS metric_key   
    , tx.solution
    , tx.period
    , 'day'                                          AS period_grain   
    , CASE
          WHEN pm.value = 0 THEN NULL                
          ELSE tx.value / pm.value                   
      END                                            AS value
FROM transactions tx
LEFT JOIN processing_merchants pm
  ON tx.solution = pm.solution
 AND tx.period = pm.period
