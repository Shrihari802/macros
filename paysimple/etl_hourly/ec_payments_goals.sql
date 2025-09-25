{{ config(materialized='table', dist="even") }}

SELECT
      *
FROM paysimple.dbt_paysimple.ec_payments_goals
