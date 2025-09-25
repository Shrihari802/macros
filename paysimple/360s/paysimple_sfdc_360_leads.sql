{{ config(materialized='table', dist="even") }}

SELECT 
-- dates
    l.created_date::DATE                                     AS lead_date
  , marketing_qualified_date_time_c::DATE                  AS mql_date

-- IDs
  , l.id                                                     AS lead_id
  , l.name
  , record_type_id
  , marketo_record_type_c                                 

-- attribution
  , lead_source
  , last_lead_source_c
  , partner_name_c                                         AS reseller
  , mkto_2_original_search_engine_c
  , search_engine_c                                        AS search_engine
  , campaign_c                                             AS campaign
  , referring_site_cookie_c                                AS referring_site_cookie
  , keyword_c                                              AS keyword
  , keyword_mta_1_c
  , lead_cookie_c                                          AS lead_cookie
  , lead_source_description_c                              AS lead_source_description
  , lead_cookie_mta_1_c
  , 'PaySimple'                                            AS solution

  , CASE 
        WHEN lead_source_description_c = 'Organic'
            THEN 'Organic Search'
        WHEN lead_source_description_c = 'Direct'
            THEN 'Direct'
        WHEN r.name = 'PaySimple Direct'
            THEN CASE
                     WHEN COALESCE(lead_source_c,'null') NOT IN ('Outbound','Partner','Tradeshow')
                            AND lead_cookie_c ~ 'capterra'
                        THEN 'Directory - Capterra'
                     WHEN LOWER( lead_source_c ) ~ 'telephone|chat'
                            AND COALESCE(lead_cookie_c,search_engine_c,'null') = 'null'
                        THEN 'Telephone / Chat'
                     WHEN keyword_c IS NOT NULL
                            AND COALESCE(search_engine_c,'null') !~ 'D|Z|M|FB|Q|AFF|AFF-2-MOS-Free|capterra|LK|DM'
                            AND COALESCE(LOWER( campaign_c ),'null') !~ 'display|display|similar|retargeting|remarketing'
                            AND COALESCE(referring_site_cookie_c,'null') !~ 'bing|yahoo|getapp|duck|doubleclick'
                            AND lead_source_c IS NOT NULL 
                        THEN 'Paid Search - Brand'
                      WHEN LOWER( lead_source_c ) ~ 'web site|dev web site'
                          THEN CASE
                                   WHEN lead_cookie_c ~ 'blog|ebook|partner|whitepaper|pressrel'
                                         AND COALESCE(keyword_c,search_engine_c,'undefined') = 'undefined'
                                       THEN 'Blog'
                                   WHEN COALESCE(lead_cookie_c,'null') !~ 'partner_|google|ebook'
                                         AND COALESCE(referring_site_cookie_c,'null') !~ 'search|public'
                                         AND ( COALESCE(referring_site_cookie_c,'null') ~ 'demo.pay|sandbox-payments.pay|paysimple|deleted|null'
                                               OR lead_source_c = 'Referral'
                                              )
                                         AND COALESCE( search_engine_c,'deleted') = 'deleted'
                                         AND  lead_source_c NOT IN ('Outbound','Partner','Tradeshow')
                                      THEN 'Direct'
                                   WHEN COALESCE(lead_cookie_c,'null') !~ 'public|blog|google'
                                         AND COALESCE( referring_site_cookie_c,'null' ) !~ 'null|doubleclick|mathtag|adsystem|adnxs|public|go.paysimple|getapp|capterra|facebook|linkedin|youtube|plus.google|twitter|plus.|mktg_email|go.pay|mail|paysimple'
                                         AND COALESCE(keyword_c,'undefined') = 'undefined'
                                      THEN 'Organic Search'
                                   ELSE 'Other'
                               END
                      ELSE 'Other'
                 END
        ELSE 'Other'
     END                AS sub_lead_source

, CASE 
      WHEN lead_source_description_c = 'Organic'
          THEN 'search-organic'
      WHEN lead_source_description_c = 'direct'
          THEN 'direct traffic'
      WHEN r.name = 'PaySimple Direct'
          THEN CASE
                   WHEN COALESCE(lead_source_c,'null') NOT IN ('Outbound','Partner','Tradeshow')
                          AND lead_cookie_c ~ 'capterra'
                      THEN 'directory'
                   WHEN LOWER( lead_source ) ~ 'telephone|chat'
                          AND COALESCE(lead_cookie_c,search_engine_c,'null') = 'null'
                      THEN 'telechat'
                   WHEN keyword_c IS NOT NULL
                          AND COALESCE(search_engine_c,'null') !~ 'D|Z|M|FB|Q|AFF|AFF-2-MOS-Free|capterra|LK|DM'
                          AND COALESCE(LOWER( campaign_c ),'null') !~ 'display|display|similar|retargeting|remarketing'
                          AND COALESCE(referring_site_cookie_c,'null') !~ 'bing|yahoo|getapp|duck|doubleclick'
                          AND lead_source_c IS NOT NULL 
                      THEN 'search-cpc'
                    WHEN LOWER( lead_source_c ) ~ 'web site|dev web site'
                        THEN CASE
                                 WHEN lead_cookie_c ~ 'blog|ebook|partner|whitepaper|pressrel'
                                       AND COALESCE(keyword_c,search_engine_c,'undefined') = 'undefined'
                                     THEN 'blog'
                                 WHEN COALESCE(lead_cookie_c,'null') !~ 'partner_|google|ebook'
                                       AND COALESCE(referring_site_cookie_c,'null') !~ 'search|public'
                                       AND ( COALESCE(referring_site_cookie_c,'null') ~ 'demo.pay|sandbox-payments.pay|paysimple|deleted|null'
                                             OR lead_source_c = 'Referral'
                                            )
                                       AND COALESCE( search_engine_c,'deleted') = 'deleted'
                                       AND COALESCE(lead_source_c,'null') NOT IN ('Outbound','Partner','Tradeshow')
                                    THEN 'direct traffic'
                                 WHEN COALESCE(lead_cookie_c,'null') !~ 'public|blog|google'
                                       AND COALESCE( referring_site_cookie_c,'null' ) !~ 'null|doubleclick|mathtag|adsystem|adnxs|public|go.paysimple|getapp|capterra|facebook|linkedin|youtube|plus.google|twitter|plus.|mktg_email|go.pay|mail|paysimple'
                                       AND COALESCE(keyword_c,'undefined') = 'undefined'
                                    THEN 'search-organic'
                                 ELSE 'other'
                             END
                    ELSE 'other'
               END
      ELSE 'other'
   END                AS utm_medium

, CASE
      WHEN COALESCE(lead_cookie_c,'null') !~ 'partner_|google|ebook'
            AND COALESCE(referring_site_cookie_c,'null') !~ 'search|public'
            AND ( COALESCE(referring_site_cookie_c,'null') ~ 'demo.pay|sandbox-payments.pay|paysimple|deleted|null'
                  OR lead_source_c = 'Referral'
                 )
            AND COALESCE( search_engine_c,'deleted') = 'deleted'
            AND COALESCE(lead_source_c,'null') NOT IN ('Outbound','Partner','Tradeshow')
         THEN 'other'   -- other for direct traffic
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
  
FROM {{ source('ec_fivetran_sfdc_paysimple', 'lead') }} l
LEFT JOIN {{ source('ec_fivetran_sfdc_paysimple', 'record_type') }} r
    ON l.record_type_id = r.id
 WHERE COALESCE(l.name,'null') NOT LIKE '%test%'
    AND channel_c IN ('PaySimple Direct','Franchise/Multi-Gateway')
    AND (NOT duplicate_entry_c OR duplicate_entry_c IS NULL ) 