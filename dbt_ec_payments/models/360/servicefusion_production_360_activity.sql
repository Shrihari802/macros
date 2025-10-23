{{ config(materialized='table', dist="even") }}

/*
This model creates a table that captures daily activity data for each Service Fusion account from 2022 onward. 
Each company has a daily row detailing logins, jobs, invoices, and collected payments.
*/

WITH
    customers AS (
                 SELECT DISTINCT
                     coid AS coid
                 FROM {{ source('dbt_everpro', 'servicefusion_billing') }}
                 )
  , date_list AS (
                 SELECT
                     date_key
                   , date
                   , customers.coid
                 FROM {{source('ec_dw','date')}}
                 CROSS JOIN
                 customers
                 )
  , payment   AS (
                 SELECT
                     CONCAT( year, CONCAT( LPAD( month, 2, 0 ), LPAD( day, 2, 0 ) ) ) AS date_key
                   , coid
                   , SUM( transaction_count )                                         AS transaction_count
                   , SUM( tpv )                                                       AS tpv
                 FROM {{ source('dbt_everpro', 'servicefusion_cohort_payment') }}
                 GROUP BY 1, 2
                 )
  , login     AS (
                 SELECT
                     ubase_company_id                                                                                  AS coid
                   , CONCAT( DATE_PART( 'year', login_time ), CONCAT( LPAD( DATE_PART( 'month', login_time ), 2, 0 ),
                                                                      LPAD( DATE_PART( 'day', login_time ), 2, 0 ) ) ) AS login_date
                   , COUNT( login_date )                                                                               AS count
                 FROM {{source('servicefusion','login_log')}}
                 GROUP BY 1, 2
                 )
  , job       AS (
                 SELECT
                     CONCAT( year, CONCAT( LPAD( month, 2, 0 ), LPAD( day, 2, 0 ) ) ) AS date_key
                   , ubase_company_id                                                 AS coid
                   , job_count
                   , job_volume
                 FROM {{ source('dbt_everpro', 'servicefusion_job') }}
                 )
  , invoice   AS (
                 SELECT
                     CONCAT( year, CONCAT( LPAD( month, 2, 0 ), LPAD( day, 2, 0 ) ) ) AS date_key
                   , ubase_company_id                                                 AS coid
                   , invoice_count
                   , tiv
                 FROM {{ source('dbt_everpro', 'servicefusion_invoice') }}
                 )
SELECT
    d.date_key
  , d.date
  , d.coid
  , CASE
        WHEN ll.count IS NULL
            THEN 0
            ELSE 1
    END                                AS log_in
  , COALESCE( j.job_count, 0 )         AS job_count
  , COALESCE( j.job_volume, 0 )        AS job_volume
  , COALESCE( i.invoice_count, 0 )     AS tic
  , COALESCE( i.tiv, 0 )               AS tiv
  , COALESCE( p.transaction_count, 0 ) AS tcc
  , COALESCE( p.tpv, 0 )               AS tcv
FROM date_list d
LEFT JOIN job j
          ON d.date_key = j.date_key AND d.coid = j.coid
LEFT JOIN invoice i
          ON d.date_key = i.date_key AND d.coid = i.coid
LEFT JOIN payment p
          ON d.date_key = p.date_key AND d.coid = p.coid
LEFT JOIN login ll
          ON d.date_key = ll.login_date AND d.coid = ll.coid
WHERE
    d.date >= '2022-1-1'