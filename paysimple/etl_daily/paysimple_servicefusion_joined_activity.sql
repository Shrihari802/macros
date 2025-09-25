{{ config(materialized='table', dist='even') }}

WITH fusion AS (
    SELECT
        acc.solution_sfdc_id
       , act.*
    FROM {{ ref('servicefusion_production_360_activity') }} act 
    LEFT JOIN {{ref('servicefusion_production_360_accounts')}} acc
    ON acc.coid = act.coid  
), paysimple AS (
    SELECT 
     acc.external_salesforce_id
    , act.*
    FROM {{ ref('paysimple_production_360_activity') }} act 
    LEFT JOIN {{ref('paysimple_production_360_accounts')}} acc
    ON acc.account_id = act.account_id    
)

, joint as (SELECT
     'Payments CRM:' || coalesce(p.account_id,'N/A') || ';' || 'Solution Org CRM:' || coalesce(f.solution_sfdc_id,'N/A') AS activity_account_key
    , COALESCE(f.date, p.transaction_date)                      AS transaction_date
    , p.account_id                                              AS payments_salesforce_account_id
    , p.external_salesforce_id                                  AS payments_external_salesforce_id
    , f.coid                                                    AS solution_customer_key
    , f.solution_sfdc_id                                        AS solution_salesforce_account_id
    , COALESCE(f.log_in, 0)                                     AS log_in
    , COALESCE(f.job_count, 0)                                  AS job_count
    , COALESCE(f.job_volume, 0)                                 AS job_volume
    , COALESCE(f.tic, 0)                                        AS tic 
    , COALESCE(f.tiv, 0)                                        AS tiv
    , COALESCE(f.tcc, 0)                                        AS tcc 
    , COALESCE(f.tcv, 0)                                        AS tcv
    , COALESCE(p.tpv, 0) + COALESCE(f.tpv, 0)                   AS tpv
    , COALESCE(p.txn, 0) + COALESCE(f.txn, 0)                   AS txn
FROM fusion f
FULL OUTER JOIN paysimple p
ON left(f.solution_sfdc_id,15) = left(p.external_salesforce_id,15)
AND f.date = p.transaction_date
WHERE p.account_id IS NOT NULL
     OR f.solution_sfdc_id IS NOT NULL )

SELECT *
FROM (
     SELECT *
          , CONCAT( activity_account_key, transaction_date ) AS rank_key
          , ROW_NUMBER( ) OVER (
         PARTITION BY rank_key
         ORDER BY transaction_date DESC
         )                                                   AS rn
     FROM joint
     ) deputed
WHERE
rn = 1
