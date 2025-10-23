{{ config(materialized='table', dist="even") }}

/*

This model creates a table combining Paysimple's Salesforce Account data with associated transaction records from 2022 onward
It joins 'paysimple_salesforce_account' (s) with the 'paysimple_transactions' (t) on the account ID

paysimple_salesforce_account is coming from Paysimple's warehouse - paysimple.dbt_paysimple.salesforce_account
paysimple_transactions is also coming from Paysimple's warehouse - paysimple.dbt_paysimple.transactions

*/


SELECT
    --ID's 
      s.id                                                AS salesforce_id
    , t.transaction_id
    , t.account_id 

    --Record Info
    , s.industry
    , s.sub_industry_c                                    AS sub_industry
    , s.city_c                                            AS city 
    , s.billing_state                                     AS state 
    , s.billing_country                                   AS country 
    , s.finance_bucket
    , s.ec_vertical

    --Dates
    , s.full_payment_product_date_time_c
    , convert_timezone('US/Mountain',s.exit_board_date_c) AS exit_date       
    , t.created_on                                        AS transaction_date                  
   
   --Payment Info
    , s.status_1_c         
    , t.payment_method
    , t.cc_financial_successful_count                     AS cc_successful_count  
    , t.card_brand
    , s.credit_card_processor
    , s.mcc_code 

    --Payment Volume Info                                                    
    , t.cc_volume_net_original_currency
    , t.cc_volume_net
    , t.ach_volume_net_original_currency
    , t.ach_volume_net 
    , t.cc_financial_volume
    , t.cc_financial_successful_count       
 
FROM {{source ('dbt_everpro','paysimple_salesforce_account')}} s
LEFT JOIN {{source ('dbt_everpro','paysimple_transactions')}} t 
ON s.id = t.account_id 
AND t.created_on::DATE >= '2022-01-01'