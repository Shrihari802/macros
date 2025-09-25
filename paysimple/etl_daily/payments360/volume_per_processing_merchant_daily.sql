/*
 This model calculates the average payment volume per processing merchant on a daily basis,
 defined as the ratio of total processing volume to the number of processing merchants.

 It works by:
 1. Pulling daily processing volume from total_processing_volume_daily.
 2. Pulling daily processing merchant counts from total_processing_merchants_daily.
 3. Joining the two datasets on solution and date.
 4. Calculating volume per merchant as (volume / merchants), with nulls for divide-by-zero cases.
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

-- Daily count of processing merchants per solution
, processing_merchants AS (
    SELECT
          solution
        , period
        , value
    FROM {{ ref('total_processing_merchants_daily') }}
)

-- Calculate volume per processing merchant
SELECT
      'Volume per Processing Merchant'           AS metric_key   
    , pv.solution
    , pv.period
    , 'day'                                      AS period_grain  
    , CASE
          WHEN pm.value = 0 THEN NULL           
          ELSE pv.value / pm.value              
      END                                        AS value
FROM payments_volume pv
LEFT JOIN processing_merchants pm
  ON pv.solution = pm.solution
 AND pv.period = pm.period
