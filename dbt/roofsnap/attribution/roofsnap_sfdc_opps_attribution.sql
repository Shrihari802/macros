{{ config(materialized='table', dist="even") }}

/*
collaboratemd sub lead source logic for CAR/marketing reporting:
    sub lead source = paid search & source = google & keyword contains "collaborate" then Paid Search - Google - Brand
    sub lead source = paid search & source = google then Paid Search - Google - Non-Brand
    sub lead source = paid search & source = bing & keyword contains "collaborate" then Paid Search - Bing - Brand
    sub lead source = paid search & source = bing then Paid Search - Bing - Non-Brand
    sub lead source = directory & referrer contains "360" then 360Connect
    ''''''''''''''''''''''''''' & referrer contains "capterra then Capterra
    ''''''''''''''''''''''''''' then Software Advice
    otherwise sub lead source
 */

SELECT DISTINCT
      id                                                         AS opportunity_id
    , {{ ec_mkt_sfdc_attribution( 'ec_utm_source_first_c','ec_referrer_first_c','lead_source','sub_lead_source_c','ec_utm_campaign_first_c','ec_utm_medium_first_c') }}
FROM {{source('ec_dbt_staging','sfdc_roofsnap_opportunity')}}