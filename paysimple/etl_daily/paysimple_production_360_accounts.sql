{{ config(materialized='table', dist="even") }}

WITH activations AS (
    SELECT
          t.account_id
        , MIN(CAST(t.financial_date AS DATE)) AS activation_date
    FROM {{ ref ('paysimple_transactions') }} t
    WHERE t.payment_method = 'CC'
      AND t.cc_successful_transactions = 1
    GROUP BY t.account_id
)

SELECT
      s.id                                                                 AS account_id
    , s.ec_crm_id_c                                                        AS external_salesforce_id

    -- Record Info
    , s.industry
    , s.sub_industry_c                                                     AS sub_industry
    , s.billing_city                                                       AS city
    , s.billing_state                                                      AS state
    , s.billing_country                                                    AS country
    , s.finance_bucket                                                     AS solution
    , s.ec_vertical

    -- Dates
    , CONVERT_TIMEZONE('US/Mountain', s.full_payment_product_date_time_c) AS go_live_date
    , CONVERT_TIMEZONE('US/Mountain', s.full_payment_product_date_time_c) AS software_purchase_date
    , CONVERT_TIMEZONE('US/Mountain', s.full_payment_product_date_time_c) AS payments_enable_date
    , CONVERT_TIMEZONE('US/Mountain', s.exit_board_date_c)                AS payments_exit_date
    , CONVERT_TIMEZONE('US/Mountain', s.exit_board_date_c)                AS software_exit_date

    -- Activation
    , a.activation_date

FROM {{ ref ('paysimple_salesforce_account') }} s
LEFT JOIN activations a
    ON s.id = a.account_id
