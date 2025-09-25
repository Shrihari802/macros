/*
 This model calculates the average ticket size on a daily basis,
 defined as the ratio of total processing volume to the number of transactions.

 It works by:
 1. Pulling daily processing volume from total_processing_volume_daily.
 2. Pulling daily transaction counts from total_transactions_daily.
 3. Joining the two datasets on solution and date.
 4. Calculating the average ticket as (volume / transactions), with nulls for divide-by-zero cases.
 5. Formatting the output with consistent column structure.
*/

{{ config(materialized='table', dist='even') }}

-- Daily payment processing volume per solution
WITH payments_volume AS (
    SELECT
          solution
        , period
        , value
    FROM {{ ref('total_processing_volume_daily') }}
)

-- Daily transaction count per solution
, transactions AS (
    SELECT
          solution
        , period
        , value
    FROM {{ ref('total_transactions_daily') }}
)

-- Calculate average ticket size (volume / transactions)
SELECT
      'Average Ticket'                        AS metric_key      
    , pv.solution
    , pv.period::DATE
    , 'day'                                   AS period_grain   
    , CASE
          WHEN tx.value = 0 THEN NULL        
          ELSE pv.value / tx.value            
      END                                     AS value
FROM payments_volume pv
LEFT JOIN transactions tx
  ON pv.solution = tx.solution
 AND pv.period = tx.period
