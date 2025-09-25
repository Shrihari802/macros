{{ config(materialized='table', dist="even") }}

SELECT
      *
FROM paysimple.dbt_paysimple.business_days

