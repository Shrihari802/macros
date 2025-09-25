/*
 This model calculates the wallet share for each solution on a daily basis,
 defined as the ratio of processing volume to invoice volume.

 It works by:
 1. Pulling daily processing volume from total_processing_volume_daily.
 2. Pulling daily invoice volume from total_invoice_volume_daily.
 3. Filtering out paysimple + partners.
 4. Joining the two datasets on solution and date.
 5. Calculating wallet share as (processing / invoice), with nulls for divide-by-zero cases.
 6. Formatting the output with consistent column structure and value rounding.
*/

{{ config(materialized='table', dist="even") }}

-- Daily processing volume per solution (filtered)
WITH payments_volume AS (SELECT solution
                              , period
                              , value::DECIMAL
                         FROM {{ ref('total_processing_volume_daily') }}
                         WHERE solution NOT IN (
                                                 'direct: paysimple'
                                               , 'integrated partner: third party'
                                               , 'integrated partner: zen planner'
                             ))

-- Daily invoice volume per solution (filtered)
   , invoice_volume AS (SELECT solution
                             , period
                             , value::DECIMAL
                        FROM {{ ref('total_invoice_volume_daily') }}
                        WHERE solution NOT IN (
                                                 'direct: paysimple'
                                               , 'integrated partner: third party'
                                               , 'integrated partner: zen planner'
                            ))

-- Calculate wallet share per solution and date
   , wallet_share as (SELECT 'Wallet Share' AS metric_key
                           , pv.solution
                           , pv.period
                           , 'day'        AS period_grain
                           , pv.value / NULLIF(iv.value, 0) AS value  
                      FROM payments_volume pv
                               LEFT JOIN invoice_volume iv
                                         ON pv.solution = iv.solution AND pv.period = iv.period)

-- Final output with rounded percentage value
SELECT metric_key,
       solution,
       period,
       period_grain,
       CAST(ROUND(value * 100, 2) AS NUMERIC(10, 2)) AS value
from wallet_share