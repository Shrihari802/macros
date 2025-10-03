{{ config(materialized='table', dist="even") }}

SELECT
    -- dates
    created_date::DATE                                               AS lead_date
  , mql_date_c::DATE                                                 AS mql_date
  , converted_date

    -- IDs
  , id                                                               AS lead_id
  , converted_opportunity_id                                         AS converted_opportunity_id

    -- Owner
  , owner_id                                                         AS owner_id
  , Name                                                             AS user_name
  , email                                                            AS user_email

    --Flags/Status
  , is_deleted                                                       AS is_deleted
  , is_converted                                                     AS is_converted
  , status                                                           AS lead_status
    
    --Attribution
  , a.cleaned_lead_source                                              AS lead_source
  , a.cleaned_sub_lead_source                                          AS sub_lead_source
  , a.publisher
  , a.is_brand
  , a.utm_medium
  , a.utm_source
  , 'RoofSnap'                                                       AS solution
  , ec_utm_campaign_first_c                                          AS utm_campaign
  , ec_utm_term_first_c                                              AS keyword

  , CASE
        WHEN converted_opportunity_id IS NULL
            THEN 1
        ELSE ROW_NUMBER() OVER( PARTITION BY converted_opportunity_id ORDER BY created_date ASC )    
    END                                                             AS order_lead_created_per_opportunity
    , CASE
        WHEN LOWER(mkto_71_acquisition_program_c) LIKE '%gutter%'
          THEN 'Gutter'
        WHEN LOWER(mkto_71_acquisition_program_c) LIKE '%metal%'
          THEN 'Metal'
        WHEN LOWER(mkto_71_acquisition_program_c) LIKE '%standard%'
          THEN 'Standard'
        ELSE mkto_71_acquisition_program_c
      END                                                             AS acquisition_program
    , CASE
        WHEN LOWER(COALESCE(product_interest_c, mkto_71_acquisition_program_c)) LIKE '%gutter%'
          THEN 'Gutter'
        WHEN LOWER(COALESCE(product_interest_c, mkto_71_acquisition_program_c))  LIKE '%metal%'
          THEN 'Metal'
        WHEN LOWER(COALESCE(product_interest_c, mkto_71_acquisition_program_c))  LIKE '%standard%'
          THEN 'Standard'
        ELSE mkto_71_acquisition_program_c
      END                                                             AS product_interest
    , product_services_c                                              AS product_services

FROM {{source('ec_dbt_staging','sfdc_roofsnap_lead')}} l
LEFT JOIN {{ ref('roofsnap_sfdc_leads_attribution') }} a
  ON l.id = a.lead_id 
WHERE NOT is_deleted



