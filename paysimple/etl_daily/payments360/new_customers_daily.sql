/*
 This model calculates the number of new customers per day since Jan 1 2020
 based on their software purchase date.
 
 It works by:
 1. Filtering to accounts with a software_purchase_date after Jan 1 2020
 2. Limiting to accounts associated with Service Fusion, Paysimple, and Integrated Partners (Third Party & Zen Planner).
 3. Counting distinct activity_account_key values per day and solution.
 4. Labeling the metric as "New Customers" and outputting the daily counts.
*/

{{ config(materialized='table', dist="even") }}

-- Selects and aggregates new customer counts per day for the past 730 days
WITH
    daily_data AS (
        SELECT
              TO_CHAR( CAST( software_purchase_date AS DATE ), 'YYYY-MM-DD' ) AS period
            , 'day'                                                           AS period_grain
            , LOWER(solution)                                                 AS solution
            , CAST(COUNT(DISTINCT activity_account_key) AS FLOAT)             AS value
        FROM {{ ref('paysimple_servicefusion_joined_account') }}   -- Table with joined account and solution data
        WHERE
              CAST(software_purchase_date AS DATE) >= DATE '2020-01-01'  -- including data from 2020 onward 
          AND LOWER(solution) IN (   
                'service fusion'
              , 'direct: paysimple'
              , 'integrated partner: third party'
              , 'integrated partner: zen planner'
          )   -- only including Payimple & Service Fusion 
        GROUP BY
              period
            , period_grain
            , solution
    )

-- Final output, ordering by day and solution
SELECT
      'New Customers' AS metric_key
    , solution
    , period::DATE 
    , period_grain
    , value::DECIMAL
FROM daily_data
ORDER BY period, solution DESC
