{{ config(materialized='table', dist="even") }}

WITH leads AS
                 (
                  SELECT
                        lead_date                 AS reporting_date
                      , lead_source
                      , sub_lead_source
                      , publisher
                      , is_brand
                      , 'n/a'::VARCHAR            AS opportunity_age
                      , 'n/a'::VARCHAR            AS opportunity_owner                      
                      , utm_medium
                      , utm_source
                      , solution
                      , COUNT( DISTINCT lead_id )          AS leads
                  FROM {{ source ('dbt_everpro','roofsnap_sfdc_360_leads') }}
                  WHERE order_lead_created_per_opportunity = 1
                  GROUP BY 1,2,3,4,5,6,7,8,9,10
                 ),
                 
     mqls AS
                 (
                  SELECT
                        mql_date                  AS reporting_date
                      , lead_source
                      , sub_lead_source
                      , publisher
                      , is_brand
                      , 'n/a'::VARCHAR            AS opportunity_age
                      , 'n/a'::VARCHAR            AS opportunity_owner                      
                      , utm_medium
                      , utm_source
                      , solution
                      , COUNT( DISTINCT lead_id )          AS mqls
                  FROM {{ source('dbt_everpro','roofsnap_sfdc_360_leads') }}
                  WHERE mql_date IS NOT NULL
                      AND COALESCE(lead_status,'null') <> 'Disqualified'
                      AND order_lead_created_per_opportunity = 1
                  GROUP BY 1,2,3,4,5,6,7,8,9,10
                 ),
     sals AS
                 (
                  SELECT
                        o.sal_date                           AS reporting_date
                      , lead_source
                      , sub_lead_source
                      , publisher
                      , is_brand
                      , opportunity_age
                      , opportunity_owner                        
                      , utm_medium
                      , utm_source
                      , solution
                      , COUNT( DISTINCT o.opportunity_id )   AS sals
                  FROM {{ source('dbt_everpro','roofsnap_sfdc_360_opportunities') }} o 
                  WHERE type = 'New Business'
                  AND opportunity_account_order = 1
                  GROUP BY 1,2,3,4,5,6,7,8,9,10
                 ),
     sqls AS
                 (
                  SELECT
                        o.sql_date                            AS reporting_date
                      , lead_source
                      , sub_lead_source
                      , publisher
                      , is_brand
                      , opportunity_age
                      , opportunity_owner                        
                      , utm_medium
                      , utm_source
                      , solution
                      , COUNT( DISTINCT o.opportunity_id )    AS sqls
                  FROM {{ source('dbt_everpro','roofsnap_sfdc_360_opportunities') }} o 
                      WHERE o.sql_date IS NOT NULL
                        AND o.type = 'New Business' 
                        AND opportunity_account_order = 1
                  GROUP BY 1,2,3,4,5,6,7,8,9,10
                 ),
     closed_wons AS
                 (
                  SELECT
                        o.close_date                           AS reporting_date
                      , o.lead_source
                      , o.sub_lead_source
                      , o.publisher
                      , o.is_brand
                      , opportunity_age
                      , opportunity_owner                        
                      , o.utm_medium
                      , o.utm_source
                      , o.solution
                      , COUNT( DISTINCT o.opportunity_id )   AS closed_wons
                      , SUM( o.amount )                      AS closed_won_mrr
                      , SUM( o.mrr )                         AS mrr_bookings
                  FROM {{ source('dbt_everpro','roofsnap_sfdc_360_opportunities') }} o 
                      WHERE o.opportunity_status = 'Closed Won'
                      AND o.type = 'New Business' 
                      AND o.close_date IS NOT NULL
                      AND o.opportunity_account_order = 1
                  GROUP BY 1,2,3,4,5,6,7,8,9,10
                                    )

SELECT
      reporting_date
    , lead_source
    , sub_lead_source
    , publisher
    , is_brand
    , opportunity_age
    , opportunity_owner      
    , utm_medium
    , utm_source
    , solution
    , SUM( leads )              AS leads
    , SUM( mqls )               AS mqls
    , SUM( sals )               AS sals
    , SUM( sqls )               AS sqls
    , SUM( closed_wons )        AS closed_wons
    , SUM( mrr_bookings )       AS mrr_bookings

FROM leads
FULL OUTER JOIN mqls
                USING ( reporting_date, solution, lead_source, sub_lead_source, publisher, is_brand, opportunity_age, opportunity_owner, utm_medium, utm_source )
FULL OUTER JOIN sals
                USING ( reporting_date, solution, lead_source, sub_lead_source, publisher, is_brand, opportunity_age, opportunity_owner, utm_medium, utm_source )
FULL OUTER JOIN sqls
                USING ( reporting_date, solution, lead_source, sub_lead_source, publisher, is_brand, opportunity_age, opportunity_owner, utm_medium, utm_source )
FULL OUTER JOIN closed_wons
                USING ( reporting_date, solution, lead_source, sub_lead_source, publisher, is_brand, opportunity_age, opportunity_owner, utm_medium, utm_source )
GROUP BY 1,2,3,4,5,6,7,8,9,10
