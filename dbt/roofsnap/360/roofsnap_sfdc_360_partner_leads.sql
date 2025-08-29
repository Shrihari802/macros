{{ config(materialized='table', dist="even") }}

/* converting list of affiliated partners to an array */

WITH partner_array AS (
                        SELECT 
                              id                                         AS lead_id
                            , CASE
                                  WHEN TRIM(LOWER( lead_source )) ~ 'partner|trade|.*event'
                                      THEN SPLIT_TO_ARRAY( COALESCE(sub_lead_source_c,'Unattributed')||';'||
                                                           COALESCE(active_partnership_c,'null')
                                                           ,';') 
                                  ELSE SPLIT_TO_ARRAY( active_partnership_c,';') 
                              END                                       AS partner_array
                        FROM {{ source('ec_dbt_staging', 'sfdc_roofsnap_lead') }}
                      )

/* unnesting the partner array */

, lead_partners AS (
                    SELECT
                          lead_id
                        , TRIM(TRIM( '"' FROM partner_affiliation::TEXT ))                 AS partner
                        , COALESCE( COUNT(partner_affiliation ) OVER( PARTITION BY lead_id), 1 )       AS total_lead_id_rows
                    FROM partner_array p
                      LEFT JOIN p.partner_array AS partner_affiliation  -- joining array values explodes values into own row
                        ON TRUE
                    WHERE COALESCE(TRIM(TRIM( '"' FROM partner_affiliation::TEXT )),'null') <> 'null' 
                  )

, clean_partners AS (
                    SELECT DISTINCT
                          lead_id
                        , {{ ec_mkt_clean_partner('partner') }} AS partner
                        , COALESCE( COUNT( partner ) OVER( PARTITION BY lead_id), 1 )       AS total_lead_id_rows
                    FROM lead_partners
                  )                  

SELECT DISTINCT
    l.*

    -- Partner/Metric Distribution -- creating a distributed count based on number of rows per lead_id
  , COUNT( partner ) OVER( PARTITION BY l.lead_id )                AS total_affiliated_partners
  , COALESCE( 
              1.0 / NULLIF( 
                            COUNT( l.lead_id ) OVER( PARTITION BY l.lead_id ) 
                          ,0)
            , 1 )                                                  AS distributed_count  
  , CASE
        WHEN COALESCE(partner,'None') = 'None'
            THEN 0
        ELSE 1                                                              
    END                                                            AS affiliated_count
  , CASE
        WHEN partner = COALESCE({{ ec_mkt_clean_partner('l.sub_lead_source') }},'Unattributed')
            THEN 'Direct Partner'
        ELSE 'Affiliated Partner'
    END                                                            AS partner_type
  , partner
FROM {{ ref('roofsnap_sfdc_360_leads') }} l
LEFT JOIN clean_partners p
  ON l.lead_id = p.lead_id                                            


    
