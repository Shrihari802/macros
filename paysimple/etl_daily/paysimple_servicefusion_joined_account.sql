{{ config(materialized='table', dist="even") }}

WITH paysimple_accounts AS (
    SELECT
           ps.account_id  
         , ps.external_salesforce_id               
         , NULL                        AS is_active 
         , ps.industry
         , ps.sub_industry
         , ps.city
         , ps.state
         , ps.country
         , ps.solution
         , ps.ec_vertical
         , ps.go_live_date
         , ps.software_purchase_date
         , ps.payments_enable_date
         , ps.payments_exit_date
         , ps.software_exit_date
         , ps.activation_date
    FROM {{ ref('paysimple_production_360_accounts') }} ps 
)
, servicefusion_accounts AS (
    SELECT
          sf.coid                                         AS account_id 
        , sf.solution_sfdc_id                             AS salesforce_id   
        , sf.isactive
        , sf.plan_term
        , sf.cancellationdate
        , sf.nextbilldate
        , sfdca.pendo_service_fusion_purchase_date_c::date as software_purchase_date 
        , sf.country
        , sf.state
        , sf.city
        , sf.industry
        , sf.currency
        , sf.exit_software_date                             
        , sf.vertical
        , sf.solution_org
        , sf.implementation_status                  
        , sf.gateway
        , sf.payment_enable_date
        , sf.customer_churn
        , sf.exit_software_date                            AS software_exit_date
    FROM {{ ref('servicefusion_production_360_accounts') }} sf 
	LEFT JOIN {{ source('ftc_evc_evp_sfu_sfc', 'account') }} sfdca on sf.solution_sfdc_id = sfdca.id
)

, joined_accounts AS (
    SELECT
         'Payments CRM:' || coalesce(ps.account_id,'N/A') || ';' || 'Solution Org CRM:' || coalesce(sf.salesforce_id,'N/A') AS activity_account_key
        , ps.account_id                                                  AS payments_salesforce_account_id
        , sf.account_id                                                  AS solution_customer_key
        , ps.external_salesforce_id                                      AS payments_external_salesforce_id
        , sf.salesforce_id                                               AS solution_salesforce_account_id
        , COALESCE(ps.industry, sf.industry)                             AS industry
        , ps.sub_industry
        , COALESCE(ps.city, sf.city)                                     AS city
        , COALESCE(ps.state, sf.state)                                   AS state
        , COALESCE(ps.country, sf.country)                               AS country
        , COALESCE(ps.solution, sf.solution_org)                         AS solution
        , COALESCE(ps.ec_vertical, sf.vertical)                          AS vertical
        , CASE WHEN sf.solution_org IS NOT NULL
                THEN sf.software_purchase_date
                WHEN ps.solution in ('Direct: PaySimple','Integrated Partner: Third Party','Integrated Partner: Zen Planner')
                THEN ps.software_purchase_date
               ELSE NULL
               END                                                       AS software_purchase_date
        , LEAST(ps.payments_enable_date, sf.payment_enable_date)         AS payments_enable_date
        , LEAST(sf.software_exit_date, ps.payments_exit_date)            AS payments_exit_date
        , LEAST(sf.exit_software_date, ps.software_exit_date)            AS software_exit_date
        , ps.activation_date
    FROM servicefusion_accounts sf
    FULL OUTER JOIN paysimple_accounts ps
        ON left(sf.salesforce_id,15) = left(ps.external_salesforce_id,15)
    WHERE ps.account_id IS NOT NULL
       OR sf.salesforce_id IS NOT NULL
)

SELECT *
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY activity_account_key
            ORDER BY software_purchase_date DESC NULLS LAST
        ) AS rn
    FROM joined_accounts
) deduped
WHERE rn = 1