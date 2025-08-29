{% macro ec_mkt_sfdc_attribution(utm_source_col, utm_referrer_col, lead_source_col, sub_lead_source_col, utm_campaign_kw_col, utm_medium_col) %}

-- lead source

COALESCE(
         DECODE(
                REGEXP_SUBSTR( LOWER( {{ lead_source_col }} )
                             ,'abm/list|event|trade.*show|cross.*sell|referral|natural search|media partner|partner|pmax|performance max'
                         )
                , 'abm/list', 'ABM / List'
                , 'event', 'Trade Show / Event'
                , 'tradeshow', 'Trade Show / Event'
                , 'trade show', 'Trade Show / Event'
                , 'referral', 'Referral'
                , 'natural search', 'Digital'
                , 'cross-sell', 'Subsidiary Cross-Sell'
                , 'cross sell', 'Subsidiary Cross-Sell'
                , 'media partner', 'Media Partners'
                , 'partner', 'Partners'
                , 'pmax', 'Digital'
                , 'performance max', 'Digital'
              , {{ lead_source_col }} -- Default value if no match
            )
        , '[blank]' )           AS cleaned_lead_source

-- sub lead source

, CASE
      WHEN {{ sub_lead_source_col }} ~ 'Paid Search'
          THEN CASE 
                   WHEN LOWER( {{ utm_campaign_kw_col }} ) ~ 'collab|33|sharp|good|bold|dyna|360|salus|connects|rhino|guild|point|snap|lobby|guild|pick|emh|star|qiigo|chrono|updox|md|paysimple|timely|studio|salon|sis|socius|fusion'
                        OR 
                        (  LOWER( {{ utm_campaign_kw_col }} ) NOT LIKE '%non%brand%'
                           AND LOWER({{ utm_campaign_kw_col }}) LIKE '%brand%' 
                        )
                      THEN CASE
                               WHEN COALESCE( {{ utm_source_col }}, 'null' ) || COALESCE( {{ utm_referrer_col }}, 'null' ) ~ 'google|adwords' 
                                   THEN 'Paid Search - Google - Brand'
                               WHEN COALESCE( {{ utm_source_col }}, 'null' ) || COALESCE( {{ utm_referrer_col }}, 'null' ) ~ 'bing' 
                                   THEN 'Paid Search - Bing - Brand'
                               ELSE 'Paid Search - Brand'
                          END
                   ELSE CASE 
                            WHEN COALESCE( {{ utm_source_col }}, 'null' ) || COALESCE( {{ utm_referrer_col }}, 'null' ) ~ 'google|adwords' 
                                THEN 'Paid Search - Google - Non-Brand'
                            WHEN COALESCE( {{ utm_source_col }}, 'null' ) || COALESCE( {{ utm_referrer_col }}, 'null' ) ~ 'bing' 
                                THEN 'Paid Search - Bing - Non-Brand'
                            ELSE 'Paid Search - Non-Brand'
                        END 
                END
      ELSE COALESCE(
                   DECODE(
                          REGEXP_SUBSTR(  LOWER( COALESCE( {{ sub_lead_source_col }}, 'null' )  ) || LOWER( COALESCE( {{ lead_source_col }}, 'null' ) )
                                       ,'social media|directory|direct|performance max|organic|mntn|directories|cold call|telemarketing|natural search|pmax'
                                     )
                          , 'social media', 'Organic Social'
                          , 'directory', 'Directory'
                          , 'direct', 'Direct Traffic'
                          , 'performance max', 'PMAX'
                          , 'organic', 'Organic Search'
                          , 'mntn', 'CTV'
                          , 'directories', 'Directory'
                          , 'cold call', 'Telephone'
                          , 'telemarketing', 'Telephone'
                          , 'natural search', 'Organic Search'
                          , 'pmax', 'Google'
                        , {{ sub_lead_source_col }} -- Default value if no match
                      )
                  , '[blank]' )
END                             AS cleaned_sub_lead_source

-- publisher
, DECODE(
          REGEXP_SUBSTR( LOWER( COALESCE( {{ utm_source_col }}, 'null' ) ) || LOWER( COALESCE( {{ sub_lead_source_col }}, 'null' ) ) || LOWER( COALESCE( {{ utm_referrer_col }}, 'null' ) )
                    ,'google|bing|yahoo|capterra|mntn|linkedin|facebook|getapp|g2|software.*advice|select.*hub|buyer.*zone|360.*connect|reddit|display|youtube|pmax|performance max|adwords|mvf|video|prospect.*path'
                   )
          , 'google', 'Google'
          , 'bing', 'Bing'
          , 'yahoo', 'Yahoo'
          , 'mntn', 'MNTN'
          , 'capterra', 'Capterra'
          , 'linkedin', 'LinkedIn'
          , 'facebook', 'Facebook'
          , 'getapp', 'GetApp'
          , 'g2', 'G2'
          , 'softwareadvice', 'Software Advice'
          , 'software advice', 'Software Advice'
          , 'selecthub', 'SelectHub'
          , 'select hub', 'SelectHub'
          , 'buyerzone', 'BuyerZone'
          , 'buyer zone', 'BuyerZone'
          , '360connect', '360Connect'
          , '360 connect', '360Connect'
          , 'reddit', 'Reddit'
          , 'display', 'Google'
          , 'youtube', 'Google'
          , 'pmax', 'Google'
          , 'performance max', 'Google'
          , 'adwords', 'Google'
          , 'mvf', 'MVF'
          , 'video', 'Google'
          , 'prospect path', 'Prospect Path'
          , 'prospectpath', 'Prospect Path'
        , 'N/A' -- Default value if no match
      )         AS publisher

-- utm_medium
, CASE
       WHEN {{ sub_lead_source_col }} = {{ utm_medium_col }} -- some solutions don't have utm fields, so would be using the same field as sub lead source parameter
            THEN CASE
                     WHEN {{ sub_lead_source_col }} = 'Social Media'
                         THEN 'social-organic'
                     WHEN LOWER( {{ sub_lead_source_col }} ) ~ 'list'
                         THEN 'list'
                     WHEN LOWER( {{ sub_lead_source_col }} ) ~ 'trade.*show|event'
                         THEN 'tradeshow/event'
                     WHEN LOWER( {{ sub_lead_source_col }} ) ~ 'paid social|facebook|instagram'
                         THEN 'social-cpc'
                     WHEN {{ lead_source_col }} LIKE '%Natural Search'
                         THEN 'search-organic'
                     WHEN LOWER( {{ sub_lead_source_col }} ) ~ 'paid search|bing|google'
                         THEN 'search-cpc'
                     WHEN {{ sub_lead_source_col }} = 'Direct'
                         THEN 'direct traffic'
                     WHEN {{ sub_lead_source_col }} = 'Performance Max'
                         THEN 'pmax'
                     WHEN {{ sub_lead_source_col }} = 'Organic'
                         THEN 'search-organic'
                     WHEN {{ sub_lead_source_col }} = 'MNTN'
                         THEN 'ctv'    
                     WHEN LOWER( {{ sub_lead_source_col }} ) ~ 'directory|directories|capterra|g2|getapp|softwareadvice'
                         THEN 'directory'
                     WHEN LOWER( {{ sub_lead_source_col }} ) ~ 'youtube'
                         THEN 'video'
                     WHEN {{ sub_lead_source_col }} IN ('Partner','Partnership')
                         THEN 'partners'
                     WHEN {{ sub_lead_source_col }} IN ( 'Cold Call','Telemarketing' )
                         THEN 'telephone'
                     WHEN {{ lead_source_col }} ~ 'Referrals'
                         THEN 'referrals'
                     WHEN LOWER( {{ lead_source_col }} ) IN ('referral','referral traffic')
                         THEN 'referral traffic'
                     ELSE COALESCE( LOWER( {{ sub_lead_source_col }} ), '[blank]' )
                 END
       WHEN LOWER( {{ lead_source_col }} ) ~ 'referral'
           THEN 'referral'
       WHEN LOWER( {{ lead_source_col }} ) IN ( 'partner','partners','partnerships' )
           THEN 'partners'
       WHEN LOWER( {{ lead_source_col }} ) ~ 'event|trade.*show'
           THEN 'tradeshow/event'
       WHEN LOWER( {{ lead_source_col }} ) ~ 'cross|subsidiary'
           THEN 'cross-sell'
       WHEN LOWER( {{ lead_source_col }} ) ~ 'lead.*ad|lead.*gen|abm'
           THEN 'abm'
       WHEN NOT( COALESCE(LOWER( {{ lead_source_col }} ),'null') IN ( 'digital', 'web','website','null' ) )
            THEN COALESCE( LOWER( {{ lead_source_col }} ), '[blank]' )
       WHEN COALESCE( LOWER( {{ utm_medium_col }} ), 'null' ) <> 'null'  -- when there is a utm_medium value (including "NULL")
            THEN CASE
                     WHEN LOWER( {{ utm_medium_col }} ) ~ 'facebook|fb|linkedin|reddit|instagram|twitter|ig|social-cpc'
                         THEN 'social-cpc'
                     WHEN LOWER( {{ utm_source_col }} ) ~ 'facebook|fb|linkedin|reddit|instagram|twitter|ig'
                         THEN 'social-cpc'
                     WHEN LOWER( {{ utm_campaign_kw_col }} ) ~ 'video|youtube'
                         THEN 'video'
                     WHEN LOWER( {{ utm_medium_col }} ) ~ 'youtube|video'
                         THEN 'video'
                     WHEN LOWER( {{ utm_campaign_kw_col }} ) ~ 'display'
                         THEN 'display'
                     WHEN LOWER( {{ utm_campaign_kw_col }} ) ~ 'pmax|performace max'
                         THEN 'pmax'
                     WHEN LOWER( {{ utm_medium_col }} ) ~ 'pmax|performace max'
                         THEN 'pmax'
                     WHEN LOWER( {{ utm_source_col }} ) ~ 'mntn'
                         THEN 'ctv'
                     WHEN LOWER( {{ utm_medium_col }} ) ~ 'mntn'
                         THEN 'ctv'
                     WHEN LOWER( {{ utm_referrer_col }} ) ~ 'facebook|fb|linkedin|reddit|instagram|twitter|ig'
                         THEN 'social-cpc'
                     WHEN LOWER( {{ utm_source_col }} ) ~ 'google|adwords|bing'
                         THEN 'search-cpc' 
                     ELSE LOWER( {{ utm_medium_col }} )
                 END
        WHEN COALESCE( LOWER( {{ utm_medium_col }} ), LOWER({{ lead_source_col }}), 'digital' ) IN ( 'digital','null','web','website','[blank]' ) -- the digital in the coalesce will ensure that only blank or digital lead sources are included (do not want to include non-dig)
            THEN CASE
                     WHEN COALESCE( LOWER( {{ utm_source_col }} ), 'null' ) IN ('null','[blank]')
                        THEN CASE
                                WHEN LOWER( {{ utm_referrer_col }} ) ~ 'google|bing|yahoo|brave|duckduck|aol'
                                    THEN 'search-organic'
                                WHEN LOWER( {{ utm_referrer_col }} ) ~ 'facebook|fb|linkedin|reddit|instagram|twitter'
                                    THEN 'social-organic'
                                WHEN LOWER( {{ utm_referrer_col }} ) IS NOT NULL
                                    THEN 'referral traffic'
                                ELSE 'direct traffic'  
                             END
                     ELSE 'missing utm_medium'
                 END
        WHEN LOWER( {{ utm_medium_col }} ) ~ 'referral'
            THEN 'referral traffic'
        WHEN LOWER( {{ utm_source_col }} ) ~ 'email'
            THEN 'email'
        WHEN LOWER( {{ utm_medium_col }} ) ~ 'email'
            THEN 'email'
        ELSE COALESCE( LOWER( {{ utm_medium_col }} ), '[blank]' )
    END                     AS utm_medium

 , CASE
       WHEN NOT( COALESCE(LOWER( {{ lead_source_col }} ),'null') IN ( 'digital', 'web','website','null' ) )
           THEN COALESCE( LOWER( {{ sub_lead_source_col }} ), 'missing source' ) 
       WHEN COALESCE( LOWER( {{ utm_medium_col }} ), LOWER({{ lead_source_col }}), 'digital' ) IN ( 'digital', 'web','website', 'null' )   -- the digital in the coalesce will ensure that only blank or digital lead sources are included (do not want to include non-dig)
            THEN CASE
                     WHEN COALESCE( LOWER( {{ utm_source_col }} ), 'null' ) = 'null'
                        THEN CASE
                                WHEN LOWER( {{ utm_referrer_col }} ) ~ 'google|adwords'
                                    THEN 'google'
                                WHEN LOWER( {{ utm_referrer_col }} ) ~ 'bing'
                                    THEN 'bing'
                                WHEN LOWER( {{ utm_referrer_col }} ) ~ 'yahoo'
                                    THEN 'yahoo'
                                WHEN LOWER( {{ utm_referrer_col }} ) ~ 'brave'
                                    THEN 'brave'
                                WHEN LOWER( {{ utm_referrer_col }} ) ~ 'duckduckgo'
                                    THEN 'duckduckgo'
                                WHEN LOWER( {{ utm_referrer_col }} ) ~ 'aol'
                                    THEN 'aol'    
                                WHEN LOWER( {{ utm_referrer_col }} ) ~ 'facebook|fb|instagram|ig'
                                    THEN 'facebook'  
                                WHEN LOWER( {{ utm_referrer_col }} ) ~ 'twitter'
                                    THEN 'twitter'  
                                WHEN LOWER( {{ utm_referrer_col }} ) ~ 'linkedin'
                                    THEN 'linkedin'  
                                WHEN LOWER( {{ utm_referrer_col }} ) ~ 'reddit'
                                    THEN 'reddit'  
                                WHEN LOWER( {{ utm_referrer_col }} ) ~ 'youtube'
                                    THEN 'youtube'  
                                WHEN LOWER( {{ utm_referrer_col }} ) IS NOT NULL
                                    THEN LOWER( {{ utm_referrer_col }} )
                                ELSE 'n/a'
                            END 
                     ELSE LOWER( {{ utm_source_col }} )
                 END
       WHEN COALESCE( LOWER( {{ utm_medium_col }} ), 'null' ) <> 'null'
            THEN CASE
                     WHEN COALESCE( LOWER( {{ utm_medium_col }} ),'null') || COALESCE( LOWER( {{ utm_source_col }} ),'null') ~ 'fb|instagram|ig|facebook'
                         THEN 'facebook'
                     WHEN LOWER( {{ utm_medium_col }} )  ~ 'linkedin|reddit|twitter'
                         THEN LOWER( {{ utm_medium_col }} )
                    WHEN LOWER( {{ utm_source_col }} )  ~ 'twitter'
                         THEN 'twitter'
                     WHEN LOWER( {{ utm_source_col }} ) ~ 'linkedin'
                         THEN 'linkedin'
                     WHEN LOWER( {{ utm_source_col }} ) ~ 'reddit'
                         THEN 'reddit'
                     WHEN LOWER( {{ utm_source_col }} ) ~ 'adwords|google'
                         THEN 'google'
                     WHEN LOWER( {{ utm_medium_col }} ) ~ 'youtube|video'
                         THEN 'google'
                     WHEN LOWER( {{ utm_campaign_kw_col }} ) ~ 'display|pmax|performance max|video|youtube'
                         THEN 'google'
                     WHEN LOWER( {{ utm_referrer_col }} )  ~ 'twitter'
                         THEN 'twitter'
                     WHEN LOWER( {{ utm_referrer_col }} )  ~ 'fb|instagram|ig|facebook'
                         THEN 'facebook'
                     WHEN LOWER( {{ utm_referrer_col }} ) ~ 'linkedin'
                         THEN 'linkedin'
                     WHEN LOWER( {{ utm_referrer_col }} ) ~ 'reddit'
                         THEN 'reddit'
                     WHEN LOWER( {{ utm_campaign_kw_col }} ) ~ 'capterra'
                         THEN 'capterra'
                     ELSE  COALESCE( LOWER( {{ utm_source_col }} ), 'missing utm_source' )
                 END
        WHEN COALESCE( LOWER( {{ utm_medium_col }} ),'null') || COALESCE( LOWER( {{ utm_source_col }} ),'null')  ~  'mntn'
            THEN 'mntn'
        WHEN COALESCE( LOWER( {{ utm_medium_col }} ),'null') || COALESCE( LOWER( {{ utm_source_col }} ),'null') ~ 'email'
            THEN LOWER( {{ utm_source_col }} )
        ELSE COALESCE( LOWER( {{ utm_source_col }} ), 'missing utm_source' ) 
    END         AS utm_source      

    -- is_brand
    , CASE
          WHEN LOWER( {{ utm_medium_col }} ) ~ 'cpc|ppc|paid|facebook|fb|linkedin|reddit|instagram|twitter|ig'
            THEN CASE
                     WHEN LOWER( {{ utm_campaign_kw_col }} ) ~ 'video|youtube'
                         THEN 'n/a'
                     WHEN LOWER( {{ utm_campaign_kw_col }} ) ~ 'display'
                         THEN 'n/a'
                     WHEN LOWER( {{ utm_campaign_kw_col }} ) ~ 'pmax|performace max'
                         THEN 'n/a'
                     WHEN COALESCE( LOWER( {{ utm_source_col }} ), 'null' ) 
                            || COALESCE( LOWER( {{ utm_referrer_col }} ), 'null' )
                            || COALESCE( LOWER( {{ utm_medium_col }} ), 'null' ) ~ 'facebook|fb|linkedin|reddit|instagram|twitter|ig'
                         THEN 'n/a'
                     WHEN LOWER( {{ utm_campaign_kw_col }} ) LIKE '%brand%' 
                         THEN CASE
                                  WHEN LOWER( {{ utm_campaign_kw_col }} ) LIKE '%non%brand%' 
                                      THEN 'false'
                                  ELSE 'true'
                              END
                     WHEN LOWER( {{ utm_campaign_kw_col }} ) ~ 'collab|33|sharp|good|bold|dyna|360|salus|connects|rhino|guild|point|snap|lobby|guild|pick|emh|star|qiigo|chrono|updox|md|paysimple|timely|studio|salon|sis|socius|fusion'
                         THEN 'true'
                     ELSE 'false'
                 END
          ELSE 'n/a'
END                                      AS is_brand

{% endmacro %}
