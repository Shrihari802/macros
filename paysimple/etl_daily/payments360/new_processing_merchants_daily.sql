/*
This model calculates the daily number of new processing merchants (clients with 
their first transaction) starting from January 1, 2020.

It works by:
1. Selecting transactions from PaySimple with associated solution details.
2. Filtering for relevant solution values.
3. Identifying the first transaction date per client.
4. Comparing each transaction to the client's first transaction date.
5. Counting clients whose first transaction occurred on each day, grouped by solution.
6. Labeling the metric as "New Processing Merchants".
*/

{{ config(materialized='table', dist="even") }}

-- Step 1: Base transaction data with solution info
WITH base_data AS (
    SELECT
          t.client_id
        , LOWER(a.finance_bucket)    AS account_finance_bucket
        , CAST(t.created_on AS DATE) AS transaction_date
    FROM {{ source ('dbt_everpro','paysimple_transactions')}} t
    LEFT JOIN {{ source ('dbt_everpro','paysimple_salesforce_account')}} a
           ON t.account_id = a.id
    WHERE t.created_on >= TIMESTAMP '2020-01-01'
      AND (
             LOWER(a.finance_bucket) = 'service fusion'
          OR LOWER(a.finance_bucket) = 'direct: paysimple'
          OR LOWER(a.finance_bucket) = 'integrated partner: third party'
          OR LOWER(a.finance_bucket) = 'integrated partner: zen planner'
      )
)

-- Step 2: Determine first transaction date per record
, first_seen AS (
    SELECT
          client_id
        , MIN(transaction_date) AS first_transaction_date
    FROM base_data
    GROUP BY client_id
)

-- Step 3: Join original data with first seen data
, merged AS (
    SELECT
          b.transaction_date
        , b.account_finance_bucket
        , b.client_id
        , f.first_transaction_date
    FROM base_data b
    JOIN first_seen f
      ON b.client_id = f.client_id
)

-- Step 4: Final output, ordered by day and solution 
SELECT
      'New Processing Merchants'            AS metric_key
    , account_finance_bucket                AS solution
    , transaction_date::DATE                AS period
    , 'day'                                 AS period_grain
    , COUNT(DISTINCT CASE
                      WHEN transaction_date = first_transaction_date
                          THEN client_id
                    END)::DECIMAL           AS value
FROM merged
GROUP BY 
      metric_key
    , solution
    , period
    , period_grain
ORDER BY period, solution
