 /*
 This model identifies the number of payment-enabled accounts that are inactive 
 (have had no transactions in the previous 30 days) for each day since Jan 1, 2020. 
 
 It works by:
 1. creating a list of days since Jan 1 2020
 2. Filtering for accounts with a valid payments_enable_date.
 3. Extracting distinct transaction dates per account.
 4. Finding accounts that were enabled on a given day but had no activity in the 
    prior 30 days.
 5. Aggregating the count of such inactive accounts by solution and date.
 */

{{ config(materialized='table', dist="even") }}

-- Generate last days since 2020
WITH RECURSIVE day_sequence(period) AS (
    SELECT CURRENT_DATE::DATE AS period
    UNION ALL
    SELECT (period - INTERVAL '1 day')::DATE
    FROM day_sequence
    WHERE period > DATE '2020-01-01'
)

-- All enabled accounts with a valid payments_enable_date
, enabled_accounts AS (
    SELECT
          account_id
        , solution
        , CAST(payments_enable_date AS DATE) AS payments_enable_date
        , CAST(payments_exit_date   AS DATE) AS payments_exit_date
    FROM {{ ref('paysimple_production_360_accounts') }}
    WHERE payments_enable_date IS NOT NULL
)

-- All transactions per account
, account_activity AS (
    SELECT DISTINCT
          account_id
        , DATE_TRUNC('day', created_on) AS txn_date
    FROM {{ ref('paysimple_transactions') }}
    WHERE created_on IS NOT NULL
)

-- Identify inactive accounts per day (no txn in prior 30 days)
, inactive_accounts_by_day AS (
    SELECT
          ds.period
        , ea.solution
        , ea.account_id
    FROM day_sequence ds
    JOIN enabled_accounts ea
      ON ea.payments_enable_date <= ds.period
     AND (ea.payments_exit_date IS NULL OR ea.payments_exit_date > ds.period)
    LEFT JOIN account_activity aa
      ON aa.account_id = ea.account_id
     AND aa.txn_date >= ds.period - INTERVAL '30 day'
     AND aa.txn_date < ds.period
    WHERE aa.account_id IS NULL
)

-- Final output, ordering by day and solution
SELECT
      'Inactive Payments Enabled'         AS metric_key
    , solution
    , TO_CHAR(period, 'YYYY-MM-DD')::DATE AS period
    , 'day'                               AS period_grain
    , COUNT(DISTINCT account_id)::DECIMAL AS value
FROM inactive_accounts_by_day
GROUP BY 1, 2, 3, 4
ORDER BY period, solution
