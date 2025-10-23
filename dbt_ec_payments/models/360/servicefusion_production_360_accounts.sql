{{ config(materialized='table', dist="even") }}

/*
This model generates a comprehensive table of all necessary dimensions for the Payment 360 project related to Service Fusion accounts, and supplies key data points for use in dashboard development
It contains column coid which is used as the primary key in Service Fusion production database (also referred to as ubase_company_id or company_id)
It also contains column solution_sfdc_id which can be used to join PaySimple salesforce data on ec_crm_id_c
*/

with pe_date as (
    SELECT
    g.ubase_company_id           AS coid
  , CAST( g.created_at AS DATE ) AS payment_enable_date
  , CASE
        WHEN mg.name = 'FusionPay'
            THEN CASE WHEN fcc.payfac_id = 1 THEN 'STAX' ELSE NULL END
            ELSE mg.name
    END                          AS gateway
FROM {{source( 'servicefusion', 'ubase_gateways' )}} g
JOIN {{source( 'servicefusion', 'master_gateways' )}} mg
     ON g.master_gateway_id = mg.id
LEFT JOIN {{source('fusionpay','fp_client_company')}} fcc
          ON g.ubase_company_id = fcc.external_id
WHERE
    gateway IS NOT NULL
)

SELECT
    CASE
        WHEN b.coid IS NULL OR b.isactive IS NULL
            THEN NULL
        WHEN b.isactive = 'T'
            THEN NULL
            ELSE
            CASE
                WHEN b.cancellationdate IS NULL
                    THEN DATEADD( DAY, -1, b.nextbilldate )
                WHEN b.cancellationdate <= b.nextbilldate
                    THEN DATEADD( DAY, -1, b.nextbilldate )
                    ELSE CASE
                             WHEN b.plan_term = 'Monthly'
                                 THEN CASE
                                          WHEN DATEADD( MONTH, 1, b.nextbilldate ) <= b.cancellationdate
                                              THEN b.cancellationdate
                                              ELSE DATEADD( DAY, -1, b.nextbilldate )
                                      END
                             WHEN b.plan_term = 'Quarterly'
                                 THEN CASE
                                          WHEN DATEADD( MONTH, 3, b.nextbilldate ) <= b.cancellationdate
                                              THEN b.cancellationdate
                                              ELSE DATEADD( DAY, -1, b.nextbilldate )
                                      END
                             WHEN b.plan_term = 'Annually'
                                 THEN CASE
                                          WHEN
                                              DATEADD( MONTH, 12, b.nextbilldate ) <= b.cancellationdate
                                              THEN b.cancellationdate
                                              ELSE DATEADD( DAY, -1, b.nextbilldate )
                                      END
                             WHEN b.plan_term = 'Semi-Annual'
                                 THEN CASE
                                          WHEN
                                              DATEADD( MONTH, 6, b.nextbilldate ) <= b.cancellationdate
                                              THEN b.cancellationdate
                                              ELSE DATEADD( DAY, -1, b.nextbilldate )
                                      END
                         END
            END
    END                           AS customer_churn /*this is customer last access to software date*/
  , b.coid
  , b.isactive
  , b.plan_term
  , b.cancellationdate
  , b.nextbilldate
  , b.signupdate                  AS software_purchase_date
  , b.country
  , b.state
  , b.city
  , b.industry
  , c.currency
  , b.cancellationdate            AS exit_software_date
  , 'Everpro'                     AS vertical
  , 'ServiceFusion'               AS solution_org
  , sfdca.implementation_status_c AS implementation_status
  , sfdca.graduation_date_c       AS go_live_date
  , sfdca.id                      AS solution_sfdc_id
  , pe.gateway
  , pe.payment_enable_date
FROM {{ source('dbt_everpro', 'servicefusion_billing') }} b
LEFT JOIN {{ source('dbt_everpro', 'servicefusion_currency') }} c
          ON b.coid = c.coid
LEFT JOIN {{ source('ec_dbt_staging', 'sfdc_servicefusion_account') }} sfdca
          ON b.coid = sfdca.pendo_service_fusion_company_id_c
LEFT JOIN pe_date pe 
          ON b.coid = pe.coid
