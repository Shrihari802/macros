{{ config(materialized='table', dist="even") }}

SELECT
    -- IDs
    a.id                                                      AS account_id

    -- Contact info
  , a.name                                                    AS opportunity_name
  , a.description                                             AS description

    -- Dates
  , a.created_date::DATE                                      AS account_created_date
  , a.initial_pay_as_you_go_date_c::DATE                      AS initial_pay_as_you_go_date
    
    -- Flags/Status
  , a.subscription_type_c                                     AS type
  , a.is_deleted                                              AS is_deleted
  , a.overall_account_status_c                                AS account_status
  , CASE 
      WHEN overall_account_status_c = 'Active - Pay As You Go'
        THEN 'Pay As You Go'
      WHEN overall_account_status_c = 'Active - Subscription'
        THEN 'Subscriber'
      ELSE 'Inactive'
    END                                                       AS subscription_type                                                 
      

    -- Sales info
  , a.braintree_next_billing_period_amount_c::FLOAT           AS amount

    --Tracking/Reporting

  , 'Roofsnap'                                                AS solution
  , a.acquisition_program_c                                   AS acquisition_program 
  , at.cleaned_lead_source                                    AS lead_source
  , at.cleaned_sub_lead_source                                AS sub_lead_source
  , at.publisher
  , at.is_brand
  , at.utm_medium
  , at.utm_source
  , a.product_interest_c                                      AS product_interest
  , a.product_services_c                                      AS product_services
  , o.ec_utm_campaign_first_c                                 AS utm_campaign
  , o.ec_utm_term_first_c                                     AS keyword
  , a.latest_mrr_c                                            AS mrr
    
FROM {{source('ec_dbt_staging','sfdc_roofsnap_account')}} a
  LEFT JOIN {{source('ec_dbt_staging','sfdc_roofsnap_opportunity')}} o
  ON a.id = o.account_id
  LEFT JOIN {{ ref('roofsnap_sfdc_accounts_attribution') }} at
  ON a.id = at.account_id 
WHERE NOT a.is_deleted



