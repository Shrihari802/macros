/*
This model calculates the total number of invoices created per day for Service Fusion. 
It transforms raw invoice data from the servicefusion_invoice into a daily-grain metric view.

It works by:

1. Selecting invoice records from Service Fusion with associated billing details.
2. Filtering out inactive records and limiting to years from 2020 onward.
3. Constructing a daily date (period) from year, month, and day columns.
4. Groups invoice data by day and solution.
5. Sums the total number of invoices for each day.
6. Labels the metric as "Total Invoice Count" with a daily grain.

*/


{{ config(materialized='table', dist='even') }}

SELECT
      'Total Invoice Count'                                                 AS metric_key
    , 'service fusion'                                                      AS solution
    , TO_CHAR( (i.year || '-' || LPAD(i.month::VARCHAR, 2, '0') || '-' || LPAD(i.day::VARCHAR, 2, '0'))::DATE , 'YYYY-MM-DD')::DATE AS period
    , 'day'                                                                 AS period_grain
    , SUM(i.invoice_count)::DECIMAL                                         AS value
FROM {{ ref('servicefusion_invoice') }} i
LEFT JOIN {{ ref('servicefusion_billing') }} b
       ON b.coid = i.ubase_company_id
WHERE isactive IS NOT NULL
  AND i.year >= 2020
GROUP BY 1,2,3,4
