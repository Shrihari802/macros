/*
This model calculates the daily total invoice volume for Service Fusion starting from 2020.

It works by:
1. Selecting invoice records and reconstructing a full date from year, month, and day columns.
2. Joining with billing data to ensure only active accounts are included.
3. Filtering for invoice data from 2020 onward.
4. Aggregating total invoice volume (TIV) by day.
5. Labeling the metric as "Total Invoice Volume".
*/


{{ config(materialized='table', dist="even") }}

SELECT
      'Total Invoice Volume'                     AS metric_key
    , 'service fusion'                           AS solution
    , TO_DATE(
          i.year || '-' 
          || LPAD(i.month::VARCHAR, 2, '0') 
          || '-' 
          || LPAD(i.day::VARCHAR, 2, '0')
      , 'YYYY-MM-DD')::DATE                            AS period
    , 'day'                                      AS period_grain
    , SUM(i.tiv)::DECIMAL                                 AS value  
FROM {{ ref ('servicefusion_invoice')}} i
LEFT JOIN {{ ref ('servicefusion_billing')}} b -- joining invoice to billing data
       ON b.coid = i.ubase_company_id
WHERE b.isactive IS NOT NULL
  AND i.year >= 2020
GROUP BY 
      metric_key
    , solution
    , period
    , period_grain
ORDER BY period
