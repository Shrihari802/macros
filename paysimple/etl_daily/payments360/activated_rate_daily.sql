/*
 This model calculates the activated rate for merchants on a daily basis, 
 defined as the ratio of processing merchants to payment-enabled accounts. 
 
 It works by:
 1. Pulling daily merchant processing counts from total_processing_merchants_daily.
 2. Pulling daily payment-enabled account counts from total_payment_enabled_accounts_daily.
 3. Joining the two datasets on solution and date.
 4. Calculating the activated rate as (processing / enabled), with nulls for divide-by-zero cases.
 5. Formatting the output with consistent column structure and value rounding.
*/

{{ config( materialized ='table', dist="even")}}

-- Daily count of processing merchants
WITH
    processing_merchants AS (
                            SELECT
                                solution
                              , period
                              , value::DECIMAL
                            FROM {{ ref('total_processing_merchants_daily') }}
                            )

-- Daily count of payment-enabled accounts
  , payment_enabled      AS (
                            SELECT
                                solution
                              , period
                              , value::DECIMAL
                            FROM {{ ref('total_payment_enabled_accounts_daily') }}
                            )

-- Calculate activated rate per solution and date
  , activated_rate      AS (
                            SELECT
                                'Activated Rate' AS metric_key
                              , pm.solution
                              , pm.period
                              , 'day'            AS period_grain
                              , pm.value / NULLIF(pe.value, 0) AS value       
                            FROM processing_merchants pm
                            LEFT JOIN payment_enabled pe
                                      ON pm.solution = pe.solution AND pm.period = pe.period
                            )

-- Final output with rounded percentage values
SELECT
    metric_key
  , solution
  , period
  , period_grain
  , CAST( ROUND( value * 100, 2 ) AS NUMERIC(10, 2) ) AS value
FROM activated_rate