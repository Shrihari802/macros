/*
This model calculates the daily number of new payment-enabled accounts that signed up for software within 60 days (KPI-006).

It works by:
1. Counting accounts where payments_enable_date is within 60 days of software_purchase_date
2. Filtering to accounts with payments_enable_date in the specified daily range
3. Aggregating by solution and date to provide daily cohort metrics
4. Labeling the metric as "New PEs - New Customer Cohort"
*/

{{ config(materialized='table', dist="even") }}

WITH
-- Service Fusion cohort accounts (daily)
cohort_solutions AS (
    SELECT
          'New PEs - New Customer Cohort'             AS metric_key
        , LOWER(solution)                             AS solution
        , CAST(payments_enable_date AS DATE)          AS period
        , 'day'                                       AS period_grain
        , COUNT(DISTINCT activity_account_key)        AS value
    FROM {{ ref('paysimple_servicefusion_joined_account') }}
    WHERE
          payments_enable_date IS NOT NULL
      AND software_purchase_date IS NOT NULL
      AND DATE_DIFF('day', software_purchase_date, payments_enable_date) BETWEEN 0 AND 60
      AND CAST(payments_enable_date AS DATE) >= DATE '2020-01-01'
      AND LOWER(solution) NOT IN (
            'direct: paysimple',
            'integrated partner: third party',
            'integrated partner: zen planner'
        )
    GROUP BY 1, 2, 3, 4
),

-- PaySimple cohort accounts (daily)
ps_cohort AS (
    SELECT
          'New PEs - New Customer Cohort'             AS metric_key
        , LOWER(solution)                             AS solution
        , CAST(payments_enable_date AS DATE)          AS period
        , 'day'                                       AS period_grain
        , COUNT(DISTINCT account_id)                  AS value
    FROM {{ ref('paysimple_production_360_accounts') }}
    WHERE
          payments_enable_date IS NOT NULL
      AND software_purchase_date IS NOT NULL
      AND DATE_DIFF('day', software_purchase_date, payments_enable_date) BETWEEN 0 AND 60
      AND CAST(payments_enable_date AS DATE) >= DATE '2020-01-01'
      AND LOWER(solution) IN (
            'direct: paysimple',
            'integrated partner: third party',
            'integrated partner: zen planner'
        )
    GROUP BY 1, 2, 3, 4
)

-- Final  combining both sources and ordering by day and solution
SELECT * FROM cohort_solutions
UNION ALL
SELECT * FROM ps_cohort
ORDER BY period, solution