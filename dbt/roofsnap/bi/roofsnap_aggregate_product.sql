{{ config(materialized='table', dist="even") }}

WITH leads AS
                 (
                  SELECT
                        lead_date                            AS reporting_date
                      , lead_source
                      , sub_lead_source
                      , publisher
                      , is_brand
                      , utm_medium
                      , utm_source
                      , solution
                      , acquisition_program
                      , keyword
                      , utm_campaign
                      , product_interest
                      , product_services
                      , COUNT( DISTINCT l.lead_id )          AS leads
                  FROM {{ source ('dbt_everpro','roofsnap_sfdc_360_leads') }} l
                  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
                 ),
                 
     mqls AS
                 (
                  SELECT
                        mql_date                             AS reporting_date
                      , lead_source
                      , sub_lead_source
                      , publisher
                      , is_brand
                      , utm_medium
                      , utm_source
                      , solution
                      , acquisition_program
                      , keyword
                      , utm_campaign
                      , product_interest
                      , product_services
                      , COUNT( DISTINCT lead_id )            AS mqls
                  FROM {{ source ('dbt_everpro','roofsnap_sfdc_360_leads') }} l
                  WHERE mql_date IS NOT NULL
                      AND lead_status <> 'Disqualified'
                  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
                 ),
     sals AS
                 (
                  SELECT
                        o.sal_date                           AS reporting_date
                      , o.lead_source
                      , o.sub_lead_source
                      , publisher
                      , is_brand
                      , utm_medium
                      , utm_source
                      , o.solution
                      , o.acquisition_program
                      , o.keyword
                      , o.utm_campaign
                      , o.product_interest
                      , o.product_services
                      , COUNT( DISTINCT o.opportunity_id )   AS sals
                  FROM {{ source('dbt_everpro','roofsnap_sfdc_360_opportunities') }} o 
                    WHERE o.type = 'New Business'
                    AND opportunity_account_order = 1
                  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
                 ),
     sqls AS
                 (
                  SELECT
                        o.sql_date                           AS reporting_date
                      , o.lead_source
                      , o.sub_lead_source
                      , publisher
                      , is_brand
                      , utm_medium
                      , utm_source
                      , o.solution
                      , o.acquisition_program
                      , o.keyword
                      , o.utm_campaign
                      , o.product_interest
                      , o.product_services
                      , COUNT( DISTINCT o.opportunity_id )   AS sqls
                  FROM {{ source('dbt_everpro','roofsnap_sfdc_360_opportunities') }}o 
                      WHERE o.sql_date IS NOT NULL
                        AND o.type = 'New Business'
                        AND opportunity_account_order = 1
                  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
                 ),
     closed_wons AS
                 (
                  SELECT
                        o.close_date                AS reporting_date
                      , o.lead_source
                      , o.sub_lead_source
                      , publisher
                      , is_brand
                      , utm_medium
                      , utm_source
                      , o.solution
                      , o.acquisition_program
                      , o.keyword
                      , o.utm_campaign
                      , o.product_interest
                      , o.product_services
                      , COUNT( DISTINCT o.opportunity_id )   AS closed_wons
                      , SUM( o.amount )             AS closed_won_mrr
                  FROM {{ source('dbt_everpro','roofsnap_sfdc_360_opportunities') }} o 
                      WHERE o.opportunity_status = 'Closed Won'
                      AND o.type = 'New Business' 
                      AND o.close_date IS NOT NULL
                      AND opportunity_account_order = 1
                  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
                  ), 

     paygo_sals AS
                 (
                  SELECT
                        o.initial_pay_as_you_go_date         AS reporting_date
                      , o.lead_source
                      , o.sub_lead_source
                      , publisher
                      , is_brand
                      , utm_medium
                      , utm_source
                      , o.solution
                      , o.acquisition_program
                      , o.keyword
                      , o.utm_campaign
                      , o.product_interest
                      , o.product_services
                      , COUNT( DISTINCT o.opportunity_id )   AS paygo_sals
                      , SUM(amount)                          AS paygo_mrr
                  FROM {{ source('dbt_everpro','roofsnap_sfdc_360_opportunities') }} o 
                    WHERE o.type = 'New Business'
                  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
                 ),

     subscribers AS
                 (
                  SELECT
                       account_created_date          AS reporting_date
                     , lead_source
                     , sub_lead_source
                     , publisher
                     , is_brand
                     , utm_medium
                     , utm_source
                     , solution
                     , acquisition_program
                     , keyword
                     , utm_campaign
                     , product_interest
                     , product_services
                     , COUNT( account_id )           AS subscribers
                     , SUM( amount )                 AS subscribers_mrr
                  FROM {{ source('dbt_everpro','roofsnap_sfdc_360_accounts') }}
                    WHERE subscription_type = 'Subscriber'
                  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
                 ) 

SELECT
      reporting_date
    , lead_source
    , sub_lead_source
    , publisher
    , is_brand
    , utm_medium
    , utm_source
    , solution
    , acquisition_program
    , keyword
    , utm_campaign
    , product_interest
    , product_services
    , SUM( leads )              AS leads
    , SUM( mqls )               AS mqls
    , SUM( sals )               AS sals
    , SUM( sqls )               AS sqls
    , SUM( closed_wons )        AS closed_wons
    , SUM( paygo_sals )         AS paygo_users
    , SUM( subscribers )        AS subscribers
    , SUM( closed_won_mrr )     AS closed_won_mrr
    , SUM( subscribers_mrr )    AS subscribers_mrr
    , SUM( paygo_mrr )          AS paygo_mrr
FROM leads
FULL OUTER JOIN mqls
                USING ( reporting_date, lead_source, sub_lead_source, solution, acquisition_program, keyword, utm_campaign, product_interest, product_services, publisher, is_brand, utm_medium, utm_source )
FULL OUTER JOIN sals
                USING ( reporting_date, lead_source, sub_lead_source, solution, acquisition_program, keyword, utm_campaign, product_interest, product_services, publisher, is_brand, utm_medium, utm_source )
FULL OUTER JOIN sqls
                USING ( reporting_date, lead_source, sub_lead_source, solution, acquisition_program, keyword, utm_campaign, product_interest, product_services, publisher, is_brand, utm_medium, utm_source )
FULL OUTER JOIN closed_wons
                USING ( reporting_date, lead_source, sub_lead_source, solution, acquisition_program, keyword, utm_campaign, product_interest, product_services, publisher, is_brand, utm_medium, utm_source )
FULL OUTER JOIN paygo_sals
                USING ( reporting_date, lead_source, sub_lead_source, solution, acquisition_program, keyword, utm_campaign, product_interest, product_services, publisher, is_brand, utm_medium, utm_source )
FULL OUTER JOIN subscribers
                USING ( reporting_date, lead_source, sub_lead_source, solution, acquisition_program, keyword, utm_campaign, product_interest, product_services, publisher, is_brand, utm_medium, utm_source )
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
