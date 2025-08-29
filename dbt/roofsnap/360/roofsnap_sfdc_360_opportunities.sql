{{ config(materialized='table', dist="even") }}

SELECT
    -- IDs
    o.id                                                      AS opportunity_id

    -- Contact info
  , o.account_id                                              AS account_id
  , o.name                                                    AS opportunity_name
  , o.description                                             AS description

    -- Dates
  , o.created_date::DATE                                      AS sal_date
  , o.sql_date_c::DATE                                        AS sql_date
  , o.close_date::DATE                                        AS close_date
  , ac.initial_pay_as_you_go_date
    
    -- Flags/Status
  , o.type                                                    AS type
  , o.stage_name                                              AS opportunity_status
  , o.is_deleted                                              AS is_deleted
  , o.is_won                                                  AS is_won
  , o.is_closed                                               AS is_closed

    -- Owner
  , o.owner_id                                                AS owner_id

    -- Sales info
  , o.amount::FLOAT                                           AS amount

    --Tracking/Reporting

  , 'Roofsnap'                                              AS solution
  , o.ec_utm_campaign_first_c                               AS utm_campaign
  , o.ec_utm_term_first_c                                   AS keyword
  , o.campaign_id                                           AS primary_campaign_source
  , o.sub_loss_reason_c                                     AS loss_reason
  , a.cleaned_lead_source                                   AS lead_source
  , a.cleaned_sub_lead_source                               AS sub_lead_source
  , o.active_partnership_c                        
  , a.publisher
  , a.is_brand
  , a.utm_medium
  , a.utm_source
  , CASE
        WHEN o.stage_name IN ( 'Closed Lost', 'Closed Won', 'Demoing', 'Negotiating', 'Presenting', 'Proposals', 'Processing Payment')
            THEN 1
        ELSE 0
    END                                                                           AS is_sql
  , CASE 
     WHEN LOWER(o.acquisition_program_c) LIKE '%gutter%'
       THEN 'Gutter'
     WHEN LOWER(o.acquisition_program_c) LIKE '%metal%'
       THEN 'Metal'
     WHEN LOWER(o.acquisition_program_c) LIKE '%standard%'
       THEN 'Standard'
     ELSE o.acquisition_program_c
    END                                                                                          AS acquisition_program
  , o.product_interest_c                                                                         AS product_interest
  , o.product_services_c                                                                         AS product_services
  , ROW_NUMBER() OVER(PARTITION BY o.id ORDER BY o.created_date, ac.initial_pay_as_you_go_date)  AS opportunity_account_order
  , ac.mrr                                                                                       AS mrr 
  , CASE 
      WHEN (o.close_date::DATE - o.created_date::DATE) <= 7   THEN '1-6 days'
      WHEN (o.close_date::DATE - o.created_date::DATE) <= 30  THEN '7-30 days'
      WHEN (o.close_date::DATE - o.created_date::DATE) <= 60  THEN '31-60 days'
      WHEN (o.close_date::DATE - o.created_date::DATE) <= 90  THEN '61-90 days'
      WHEN (o.close_date::DATE - o.created_date::DATE) <= 120 THEN '91-120 days'
      WHEN (o.close_date::DATE - o.created_date::DATE) <= 180 THEN '121-180 days'
    ELSE                                                           'Over 6 Months'
  END AS opportunity_age

  , CASE 
      WHEN (o.close_date::DATE - o.created_date::DATE) <= 7   THEN 1
      WHEN (o.close_date::DATE - o.created_date::DATE) <= 30  THEN 2
      WHEN (o.close_date::DATE - o.created_date::DATE) <= 60  THEN 3
      WHEN (o.close_date::DATE - o.created_date::DATE) <= 90  THEN 4
      WHEN (o.close_date::DATE - o.created_date::DATE) <= 120 THEN 5
      WHEN (o.close_date::DATE - o.created_date::DATE) <= 180 THEN 6
    ELSE                                                   7
  END AS opportunity_age_rank
  , u.name                                                  AS opportunity_owner 

FROM {{source('ec_dbt_staging','sfdc_roofsnap_opportunity')}} o
  LEFT JOIN {{ source('ec_dbt_staging', 'sfdc_roofsnap_user') }} u
    ON o.owner_id = u.id
  LEFT JOIN {{ ref('roofsnap_sfdc_opps_attribution') }} a
    ON o.id = a.opportunity_id 
  LEFT JOIN {{ ref('roofsnap_sfdc_360_accounts') }} ac
    ON o.account_id = ac.account_id 
    AND ac.initial_pay_as_you_go_date IS NOT NULL
WHERE NOT o.is_deleted



