{{ config(materialized='table', dist="even") }}

/* converting list of affiliated partners to an array */

WITH partner_array AS (
                        SELECT 
                              a.id                                         AS account_id
                            , CASE
                                  WHEN TRIM(LOWER( so.lead_source )) ~ 'partner|trade|event'
                                      THEN SPLIT_TO_ARRAY( COALESCE(so.sub_lead_source_c,a.sub_lead_source_c,'Unattributed')||';'||
                                                           COALESCE(so.active_partnership_c,'null')||';'||
                                                           COALESCE(a.active_partnership_c,'null')
                                                           ,';') 
                                  ELSE SPLIT_TO_ARRAY( so.active_partnership_c||';'||COALESCE(a.active_partnership_c,'null'),';') 
                              END                                       AS partner_array
                        FROM {{ source('ec_dbt_staging', 'sfdc_roofsnap_account') }} a
                        LEFT JOIN {{ source('ec_dbt_staging', 'sfdc_roofsnap_opportunity') }} so
                          ON so.account_id = a.id
                      )

/* unnesting the partner array */

, account_partners AS (
                    SELECT DISTINCT
                          account_id
                        , TRIM( '"' FROM partner_affiliation::TEXT )                                   AS partner
                    FROM partner_array p
                    LEFT JOIN p.partner_array AS partner_affiliation ON TRUE
                    WHERE COALESCE(TRIM( '"' FROM partner_affiliation::TEXT ) ,'null') <> 'null'
                  )

, clean_partners AS (
                    SELECT DISTINCT
                          account_id
                        , {{ ec_mkt_clean_partner('partner') }} AS partner
                        , COALESCE( COUNT( partner ) OVER( PARTITION BY account_id), 1 )       AS total_opportunity_id_rows
                    FROM account_partners
                  )      

SELECT
     -- IDs
    a.id                                                         AS account_id

    -- Contact info
  , a.name                                                       AS account_name
  , a.billing_state

    -- Dates
  , a.created_date::DATE                                         AS account_date
  , a.last_modified_date::DATE                                   AS last_modified_date
  , a.last_activity_date::DATE                                   AS last_activity_date
    
    -- Flags/Status
  , a.type                                                       AS account_type

    --Tracking/Reporting
  , 'RoofSnap'                                                  AS solution
  , o.lead_source                                               AS lead_source
  , COALESCE(o.sub_lead_source,a.sub_lead_source_c,'Unattributed')  AS sub_lead_source
  , COUNT( partner ) OVER( PARTITION BY a.id)                    AS total_affiliated_partners
  , COALESCE( 
              1.0 / NULLIF( 
                            COUNT( a.id ) OVER( PARTITION BY a.id) 
                          ,0)
            , 1 )                                                AS distributed_count  
  , CASE
        WHEN COALESCE(partner,'None') = 'None' 
            THEN 0
        ELSE 1                                                              
    END                                                           AS affiliated_count
  , CASE
        WHEN partner = COALESCE(o.sub_lead_source,a.sub_lead_source_c,'Unattributed')
            THEN 'Direct Partner'
        ELSE 'Affiliated Partner'
    END                                                           AS partner_type
  , COALESCE(partner,'None')                                      AS partner

FROM {{ source('ec_dbt_staging', 'sfdc_roofsnap_account') }} a
LEFT JOIN {{ ref('roofsnap_sfdc_360_opportunities') }} o
  ON o.account_id = a.id
LEFT JOIN clean_partners p
  ON a.id = p.account_id
WHERE 1=1
  AND NOT a.is_deleted
  AND a.overall_account_status_c ~ 'Subscription'

