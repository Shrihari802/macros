{{ config(materialized='table', dist="even") }}


/* converting list of affiliated partners to an array */

WITH partner_array AS (
                        SELECT 
                              so.opportunity_id                                         AS opportunity_id
                            , CASE
                                  WHEN TRIM(LOWER( so.lead_source )) ~ 'partner|trade|.*event'
                                      THEN SPLIT_TO_ARRAY( COALESCE( so.sub_lead_source,'Unattributed')||';'||
                                                           COALESCE( a.active_partnership_c, 'null')||';'||
                                                           COALESCE(so.active_partnership_c,'null')
                                                           ,';') 
                                  ELSE SPLIT_TO_ARRAY( COALESCE( a.active_partnership_c, 'null')||';'||
                                                       COALESCE(so.active_partnership_c,'null')
                                                      ,';') 
                              END                                       AS partner_array
                        FROM {{ ref('roofsnap_sfdc_360_opportunities') }} so
                        LEFT JOIN {{ source('ec_dbt_staging', 'sfdc_roofsnap_account') }} a
                          ON so.account_id = a.id 
                      )

/* unnesting the partner array */

, opp_partners AS (
                    SELECT
                          opportunity_id
                        , TRIM(TRIM( '"' FROM partner_affiliation::TEXT ))                 AS partner
                        , COALESCE( COUNT(partner_affiliation ) OVER( PARTITION BY opportunity_id), 1 )    AS total_account_id_rows
                    FROM partner_array p
                      LEFT JOIN p.partner_array AS partner_affiliation -- joining array values explodes values into own row
                        ON TRUE
                    WHERE COALESCE(TRIM(TRIM( '"' FROM partner_affiliation::TEXT )),'null') <> 'null' 
                  )

, clean_partners AS (
                    SELECT DISTINCT
                          opportunity_id
                        , {{ ec_mkt_clean_partner('partner') }} AS partner
                        , COALESCE( COUNT(partner) OVER( PARTITION BY opportunity_id), 1 )       AS total_opp_id_rows
                    FROM opp_partners
                  )                    

SELECT DISTINCT
    o.*
    -- Partner info
 , COUNT( partner ) OVER( PARTITION BY o.opportunity_id )         AS total_affiliated_partners
  , COALESCE( 
              1.0 / NULLIF( 
                            COUNT( o.opportunity_id ) OVER( PARTITION BY o.opportunity_id) 
                          ,0)
            , 1 )                                                  AS distributed_count  
  , CASE
        WHEN COALESCE(partner,'None') = 'None'
            THEN 0
        ELSE 1                                                              
    END                                                            AS affiliated_count
  , CASE
        WHEN COALESCE(partner,'None') = 'None'
            THEN 0
        ELSE o.amount                                                              
    END                                                            AS affiliated_amount
  , COALESCE( 
              1.0 / NULLIF( 
                            SUM( o.amount ) OVER( PARTITION BY o.opportunity_id) 
                          ,0)
            , 1 )                                                  AS distributed_amount
  , CASE
        WHEN partner = COALESCE({{ ec_mkt_clean_partner('o.sub_lead_source') }},'Unattributed')
            THEN 'Direct Partner'
        WHEN LOWER( o.lead_source ) ~ 'partner|trade|event'
            THEN 'Direct Partner'            
        ELSE 'Affiliated Partner'
    END                                                            AS partner_type
  , partner

FROM {{ ref('roofsnap_sfdc_360_opportunities') }} o
LEFT JOIN clean_partners p
  ON o.opportunity_id = p.opportunity_id

