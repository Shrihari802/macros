{{ config(materialized='table', dist="even") }}

-- updated 1/13 to change date fields.
with leads AS (
    SELECT
        lead_date                  AS reporting_date
      , lead_source
      , sub_lead_source
      , utm_medium
      , utm_source
      , is_brand
      , COUNT( DISTINCT lead_id )  AS leads
    FROM {{ source('dbt_everpro', 'paysimple_sfdc_360_leads')}}
    GROUP BY 1,2,3,4,5,6
),

mqls AS (
    SELECT
          sal_date                 AS reporting_date             
        , lead_source
        , sub_lead_source
        , utm_medium
        , utm_source
        , is_brand
        , COUNT( DISTINCT account_id )  AS mqls
    FROM {{ source('dbt_everpro', 'paysimple_sfdc_360_accounts')}}
    WHERE self_serve_full_serve_c = 'Full Serve'
        AND sal_date IS NOT NULL
    GROUP BY 1,2,3,4,5,6
),

sals AS (
    SELECT
          sal_date                 AS reporting_date             
        , lead_source
        , sub_lead_source
        , utm_medium
        , utm_source
        , is_brand
        , COUNT( DISTINCT account_id )  AS sals
    FROM {{ source('dbt_everpro', 'paysimple_sfdc_360_accounts')}}
    WHERE self_serve_full_serve_c = 'Full Serve'
        AND sal_date IS NOT NULL
    GROUP BY 1,2,3,4,5,6
),

sqls AS (
    SELECT
         sql_date                         AS reporting_date 
       , lead_source
       , sub_lead_source
       , utm_medium
       , utm_source
       , is_brand
       , COUNT( DISTINCT account_id ) AS sqls
    FROM {{ source('dbt_everpro', 'paysimple_sfdc_360_accounts')}}
    WHERE self_serve_full_serve_c = 'Self Serve'
        AND sal_date IS NOT NULL
    GROUP BY 1,2,3,4,5,6
),

closed_wons AS (
    SELECT
         close_date                       AS reporting_date
       , lead_source
       , sub_lead_source
       , utm_medium
       , utm_source
       , is_brand
       , COUNT( DISTINCT account_id ) AS closed_wons
    FROM {{ source('dbt_everpro', 'paysimple_sfdc_360_accounts')}}
        WHERE close_date IS NOT NULL
    GROUP BY 1,2,3,4,5,6
)

SELECT
      reporting_date
    , lead_source
    , sub_lead_source
    , utm_medium
    , utm_source
    , is_brand
    , SUM( leads )                        AS leads
    , SUM( mqls )                         AS mqls
    , SUM( sals )                         AS sals
    , SUM( sqls )                         AS sqls
    , SUM( closed_wons )                  AS closed_wons

FROM leads
FULL OUTER JOIN mqls 
    USING (reporting_date, lead_source, sub_lead_source, utm_medium,utm_source,is_brand )
FULL OUTER JOIN sals 
    USING (reporting_date, lead_source, sub_lead_source, utm_medium,utm_source,is_brand )
FULL OUTER JOIN sqls 
    USING (reporting_date, lead_source, sub_lead_source, utm_medium,utm_source,is_brand )
FULL OUTER JOIN closed_wons 
    USING (reporting_date, lead_source, sub_lead_source, utm_medium,utm_source,is_brand )
GROUP BY 1,2,3,4,5,6
