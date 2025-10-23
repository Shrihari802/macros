--{{ config(materialized='table', dist="even") }}

SELECT DATE_TRUNC('month', a.created_at)::DATE AS activity_period_date
     , 'Service Fusion'                        AS solution_org
     --a.ubase_company_id AS COID
     --g.name
     --, c.payfac_id
     , CASE
           WHEN g.name = 'TSYS (formerly Cayan)'
               THEN 'TSYS'
           WHEN c.payfac_id = 1 AND g.name = 'FusionPay'
               THEN 'FattMerchant'
           WHEN c.payfac_id = 2 AND g.name = 'FusionPay'
               THEN 'PaySimple'
    END                                        AS processor
     --, t.type
     --, DATE_PART( 'year', a.created_at )  AS year
     --, DATE_PART( 'month', a.created_at )  AS month
     --, DATE_PART( 'day', a.created_at )  AS day
     , COUNT(a.id)                             AS transaction_sum
     , SUM(a.amount)                           AS volume
     , ''                                      AS net_revenue
     , CASE
           WHEN SUM(a.amount) >= 5
               THEN COUNT(DISTINCT a.ubase_company_id)
    END                                        AS processing_merchants
     , ''                                      AS active_merchants
FROM --{{ source ('ec_fivetran_sf_prod_cdc_servicefusion','ubase_payments')}} a
       ec_fivetran_sf_prod_cdc_servicefusion.ubase_payments a

         LEFT JOIN --{{ source ('ec_fivetran_sf_prod_cdc_fusionpay','fp_client_company')}} c
                    ec_fivetran_sf_prod_cdc_fusionpay.fp_client_company c
                    ON a.ubase_company_id = c.external_id
         LEFT JOIN --{{ source ('ec_fivetran_sf_prod_cdc_servicefusion','master_gateways')}} g
                    ec_fivetran_sf_prod_cdc_servicefusion.master_gateways g
        ON a.master_gateway_id = g.id
         LEFT JOIN --{{ source ('ec_fivetran_sf_prod_cdc_servicefusion','ubase_payment_types')}} t
                    ec_fivetran_sf_prod_cdc_servicefusion.ubase_payment_types t
        ON a.ubase_payment_type_id = t.id
WHERE --a.created_at BETWEEN '1-1-20' AND '1-31-25'
      --AND FusionPay_PayFac is not null
      processor <> 'PaySimple'
GROUP BY 1, 2, 3
--LIMIT 5;
UNION
SELECT DATE_TRUNC('month', bpst.created_at)::DATE AS activity_period_date
     , 'DrChrono'                                 AS solution_org
     , 'Stripe'                                   AS processor
     , COUNT(DISTINCT bpst.id)                    AS transaction_sum
     , SUM(bpst.amount)                           AS volume
     , ''                                         AS net_revenue
     , COUNT(DISTINCT bpst.doctor_id)             AS processing_merchants
     , ''                                         AS active_merchants
FROM chronometer.dbt_staging.prodcdc_billing_patientstripetransaction bpst
         JOIN chronometer.dbt_staging.prodcdc_chronometer_doctor cd
              ON bpst.doctor_id = cd.id
--FROM chronometer.dbt_staging.prodcdc_billing_patientstripetransaction
WHERE
  --EXTRACT(YEAR FROM bpst.payment_date) = 2024
  --AND EXTRACT(MONTH FROM bpst.payment_date) = 6
    bpst.created_at >= '01-01-2020'
 -- AND bpst.created_at < '11-1-2024'
   AND bpst.status IN ('paid')
GROUP BY 1, 2, 3, 6, 8
--ORDER BY 1 DESC
UNION
SELECT DATE_TRUNC('month', sqr.created_at)::DATE AS activity_period_date
     , 'DrChrono'                                AS solution_org
     , 'Square'                                  AS processor
     , COUNT(DISTINCT sqr.id)                    AS transaction_sum
     , SUM(bcp.amount)                           AS volume
     , ''                                        AS net_revenue
     , COUNT(DISTINCT sqr.merchant_id)           AS processing_merchants
     , ''                                        AS active_merchants
FROM chronometer.dbt_staging.prodcdc_billing_squaretransaction sqr
         JOIN chronometer.dbt_staging.prodcdc_billing_cashpayment bcp
              ON sqr.cashpayment_id = bcp.id
         JOIN chronometer.dbt_staging.prodcdc_chronometer_doctor cd
              ON bcp.doctor_id = cd.id
--FROM chronometer.dbt_staging.prodcdc_billing_patientstripetransaction
WHERE
  --EXTRACT(YEAR FROM bpst.payment_date) = 2024
  --AND EXTRACT(MONTH FROM bpst.payment_date) = 6
    sqr.created_at >= '01-01-2020'
  --AND sqr.created_at < '11-1-2024'
  --AND bpst.status IN ('paid')
GROUP BY 1, 2, 3, 6
ORDER BY 2, 3, 1 ASC