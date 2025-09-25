{{ config(materialized='table', dist="even") }}

SELECT 
    --Dates
  a.created_date::DATE                                         AS sal_date
, a.application_completed_on_c::DATE                           AS sql_date
, a.full_payment_product_date_time_c::DATE                     AS close_date

--IDs
, a.id                                                         AS account_id

--attribution
, a.kpi_channel_c
, a.kpi_channel_2_0_c
, a.search_engine_c
, a.status_2_c
, a.lead_cookie_c
, a.lead_cookie_mta_1_c
, a.referring_site_url_mta_1_c
, a.name
, a.self_serve_full_serve_c                                    AS self_serve_full_serve_c

--account info
, a.record_type_name_c                                         
, a.channel_c                                                  AS channel
, a.status_2_c                                                 AS status_2
, a.lead_source_c                                              AS lead_source
, a.product_c                                                  
, a.partner_name_c                     
, a.original_reseller_c                                        AS reseller
, 'PaySimple'                                                  AS solution

, CASE
      WHEN COALESCE(lead_source_description_c ,'null') NOT IN ('Outbound','Partner','Tradeshow')
             AND COALESCE(status_2_c ,'null') NOT IN ('Additional Service','Processor Re-app', 'Throwback to ADR')
             AND lead_cookie_c ~ 'capterra'
         THEN 'Directory - Capterra'
      WHEN LOWER( lead_source ) ~ 'telephone|chat'
             AND COALESCE(status_2_c ,'null') NOT IN ('Additional Service','Processor Re-app')
             AND COALESCE(lead_cookie_c,search_engine_c,'null') = 'null'
         THEN 'Telephone / Chat'
      WHEN keyword_c IS NOT NULL
             AND COALESCE(status_2_c ,'null') NOT IN ('Additional Service','Processor Re-app')
             AND COALESCE(search_engine_c ,'null') !~ 'D|Z|M|FB|Q|AFF|AFF-2-MOS-Free|capterra|LK|DM'
             AND COALESCE(LOWER( campaign_c ) ,'null') !~ 'display|display|similar|retargeting|remarketing'
             AND COALESCE(referring_site_cookie_c ,'null') !~ 'bing|yahoo|getapp|duck|doubleclick'
             AND lead_source_c IS NOT NULL 
         THEN 'Paid Search - Brand'
       WHEN LOWER( lead_source_c ) ~ 'web site|dev web site'
           THEN CASE
                    WHEN lead_cookie_c ~ 'blog|ebook|partner|whitepaper|pressrel'
                          AND COALESCE(status_2_c ,'null') NOT IN ('Additional Service','Processor Re-app')
                          AND COALESCE(keyword_c,search_engine_c,'undefined') = 'undefined'
                        THEN 'Blog'
                    WHEN COALESCE(lead_cookie_c ,'null') !~ 'partner_|google|ebook'
                          AND COALESCE(referring_site_cookie_c ,'null') !~ 'search|public'
                          AND ( COALESCE(referring_site_cookie_c,'null') ~ 'demo.pay|sandbox-payments.pay|paysimple|deleted|null'
                                OR lead_source_c = 'Referral'
                               )
                          AND COALESCE( search_engine_c,'deleted') = 'deleted'
                          AND COALESCE(lead_source_c ,'null') NOT IN ('Outbound','Partner','Tradeshow')
                       THEN 'Direct'
                    WHEN COALESCE(lead_cookie_c ,'null') !~ 'public|blog|google'
                          AND COALESCE(status_2_c ,'null') NOT IN ('Additional Service','Processor Re-app')
                          AND COALESCE( referring_site_cookie_c,'null' ) !~ 'null|doubleclick|mathtag|adsystem|adnxs|public|go.paysimple|getapp|capterra|facebook|linkedin|youtube|plus.google|twitter|plus.|mktg_email|go.pay|mail|paysimple'
                          AND COALESCE(keyword_c,'undefined') = 'undefined'
                       THEN 'Organic Search'
                    ELSE 'Other'
                END
      ELSE 'Other'
   END                AS sub_lead_source

, CASE
      WHEN COALESCE(lead_source_description_c ,'null') NOT IN ('Outbound','Partner','Tradeshow')
             AND COALESCE(status_2_c ,'null') NOT IN ('Additional Service','Processor Re-app', 'Throwback to ADR')
             AND lead_cookie_c ~ 'capterra'
         THEN 'directory'
      WHEN COALESCE(lead_source_c,'null') ~ 'telephone|chat'
             AND COALESCE(status_2_c ,'null') NOT IN ('Additional Service','Processor Re-app')
             AND COALESCE(lead_cookie_c,search_engine_c,'null') = 'null'
         THEN 'telechat'
      WHEN keyword_c IS NOT NULL
             AND COALESCE(status_2_c ,'null') NOT IN ('Additional Service','Processor Re-app')
             AND COALESCE(search_engine_c ,'null') !~ 'D|Z|M|FB|Q|AFF|AFF-2-MOS-Free|capterra|LK|DM'
             AND COALESCE(LOWER( campaign_c ) ,'null') !~ 'display|display|similar|retargeting|remarketing'
             AND COALESCE(referring_site_cookie_c ,'null') !~ 'bing|yahoo|getapp|duck|doubleclick'
             AND lead_source_c IS NOT NULL 
         THEN 'search-cpc'
       WHEN LOWER( lead_source_c ) ~ 'web site|dev web site'
           THEN CASE
                    WHEN lead_cookie_c ~ 'blog|ebook|partner|whitepaper|pressrel'
                          AND COALESCE(status_2_c ,'null') NOT IN ('Additional Service','Processor Re-app')
                          AND COALESCE(keyword_c,search_engine_c,'undefined') = 'undefined'
                        THEN 'blog'
                    WHEN COALESCE(lead_cookie_c ,'null') !~ 'partner_|google|ebook'
                          AND COALESCE(referring_site_cookie_c ,'null') !~ 'search|public'
                          AND ( COALESCE(referring_site_cookie_c,'null') ~ 'demo.pay|sandbox-payments.pay|paysimple|deleted|null'
                                OR lead_source_c = 'Referral'
                               )
                          AND COALESCE( search_engine_c,'deleted') = 'deleted'
                          AND COALESCE(lead_source_c ,'null') NOT IN ('Outbound','Partner','Tradeshow')
                       THEN 'direct traffic'
                    WHEN COALESCE(lead_cookie_c ,'null') !~ 'public|blog|google'
                          AND COALESCE(status_2_c ,'null') NOT IN ('Additional Service','Processor Re-app')
                          AND COALESCE( referring_site_cookie_c,'null' ) !~ 'null|doubleclick|mathtag|adsystem|adnxs|public|go.paysimple|getapp|capterra|facebook|linkedin|youtube|plus.google|twitter|plus.|mktg_email|go.pay|mail|paysimple'
                          AND COALESCE(keyword_c,'undefined') = 'undefined'
                       THEN 'search-organic'
                    ELSE 'other'
                END
      ELSE 'other'
   END                AS utm_medium

  , CASE
        WHEN COALESCE(lead_cookie_c ,'null') !~ 'partner_|google|ebook'
              AND COALESCE(referring_site_cookie_c ,'null') !~ 'search|public'
              AND ( COALESCE(referring_site_cookie_c,'null') ~ 'demo.pay|sandbox-payments.pay|paysimple|deleted|null'
                    OR lead_source_c = 'Referral'
                   )
              AND COALESCE( search_engine_c,'deleted') = 'deleted'
              AND COALESCE(lead_source_c ,'null') NOT IN ('Outbound','Partner','Tradeshow')
           THEN 'other'
      ELSE DECODE(
                  REGEXP_SUBSTR( COALESCE(LOWER(lead_cookie_c ),'null') || COALESCE(LOWER( referring_site_cookie_c ),'null') || COALESCE(LOWER( search_engine_c ),'null')
                               ,'google|bing|yahoo|duck|aol|brave|baidoo|capterra|linkedin|facebook|zoura|adwords|youtube|reddit'
                             )  
                  , 'google', 'google'
                  , 'bing', 'bing'
                  , 'yahoo', 'yahoo'
                  , 'duck', 'duckduckgo'
                  , 'aol', 'aol'
                  , 'brave', 'brave'
                  , 'baidoo', 'baidoo'
                  , 'capterra', 'capterra'
                  , 'linkedin','linkedin'
                  , 'facebook','facebook'
                  , 'zoura','zoura'
                  , 'adwords','google'
                  , 'youtube', 'google'
                  , 'reddit', 'reddit'
                , 'other' -- Default value if no match
              )          
  END                 AS utm_source

, CASE
      WHEN LOWER( keyword_c ) ~ 'pay.*simple'
          THEN 'true'
      WHEN LOWER( keyword_c ) IS NOT NULL
          THEN 'false'
      ELSE 'other'
  END                 AS is_brand


FROM {{ source ('ec_fivetran_sfdc_paysimple','account')}} a
  WHERE COALESCE(name,'null') NOT LIKE '%test%'
  AND a.kpi_channel_c = 'Direct: Branded Marketing'
