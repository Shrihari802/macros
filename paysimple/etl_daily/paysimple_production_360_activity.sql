{{ config(materialized='table', dist="even") }}

WITH
customers           AS (
                           SELECT DISTINCT
                               account_id AS acoountid
                           FROM {{source('dbt_everpro','paysimple_transactions')}}
                           where account_id is not null
                           ),
date_list           AS (
                           SELECT
                               date_key
                             , date
                             , customers.acoountid as account_id
                           FROM {{source('ec_dw','date')}}
                           CROSS JOIN
                           customers
                           ),
 transactions_by_day AS (
        SELECT
              t.account_id
            , CAST(t.financial_date AS DATE)                           AS transaction_date
            , SUM(t.cc_financial_volume)                               AS cc_financial_volume
            , SUM(t.cc_financial_successful_count)                     AS cc_financial_successful_count
        FROM {{source('dbt_everpro','paysimple_transactions')}} t 
        WHERE t.payment_method = 'CC'
        AND transaction_date >= '2022-01-01'
        GROUP BY t.account_id, transaction_date
    ),
ps_sfdc_account as (
    SELECT  
          id
        , ec_crm_id_c --/*solution org salesforce account id*/
        , external_id_c --  /*solution org merchant key */
    FROM {{source('dbt_everpro','paysimple_salesforce_account')}}   
)

SELECT
     s.id
    , s.ec_crm_id_c --/*solution org salesforce account id*/
    , s.external_id_c
    , d.date                                                          AS transaction_date
    , d.account_id
    , COALESCE(t.cc_financial_volume, 0)                              AS tpv
    , COALESCE(t.cc_financial_successful_count, 0)                    AS txn
FROM date_list d
LEFT JOIN transactions_by_day t
          ON d.account_id = t.account_id and d.date = t.transaction_date
LEFT JOIN ps_sfdc_account s 
ON t.account_id = s.id
where d.date >= '2022-1-1'