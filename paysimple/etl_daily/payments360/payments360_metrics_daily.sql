/*
This model creates a unified table containing the output from all of the daily grain payments360 models.
It contains 5 columns:
    metric_key - the name of the reported metric
    solution - the solution name (lowercase)
    period - the date associated with the reported value
    period_grain - specifies the granularity of the data reported - will be 'day' for every value here
    value - the reported value 
*/ 

{{ config(materialized='table', dist='even') }}

SELECT * FROM {{ ref('total_payment_enabled_accounts_daily') }} 
UNION ALL
SELECT * FROM {{ ref('inactive_payment_enabled_accounts_daily') }} 
UNION ALL
SELECT * FROM {{ ref('new_customers_daily') }} 
UNION ALL
SELECT * FROM {{ ref('new_payment_enabled_accounts_daily') }} 
UNION ALL
SELECT * FROM {{ ref('new_processing_merchants_daily') }} 
UNION ALL
SELECT * FROM {{ ref('total_invoice_count_daily') }} 
UNION ALL
SELECT * FROM {{ ref('total_invoice_volume_daily') }} 
UNION ALL
SELECT * FROM {{ ref('total_processing_merchants_daily') }} 
UNION ALL
SELECT * FROM {{ ref('total_processing_volume_daily') }} 
UNION ALL
SELECT * FROM {{ ref('total_transactions_daily') }} 
UNION ALL
SELECT * FROM {{ ref('wallet_share_daily') }}
UNION ALL
SELECT * FROM {{ ref('average_ticket_daily') }}
UNION ALL
SELECT * FROM {{ ref('volume_per_processing_merchant_daily') }}
UNION ALL
SELECT * FROM {{ ref('transactions_per_processing_merchant_daily') }}
UNION ALL
SELECT * FROM {{ ref('attach_rate_daily') }}
UNION ALL
SELECT * FROM {{ ref('activation_rate_daily') }}
UNION ALL
SELECT * FROM {{ ref('activated_rate_daily') }}
UNION ALL
SELECT * FROM {{ ref('attach_rate_new_payments_enabled_daily') }}
UNION ALL
SELECT * FROM {{ ref('estimated_revenue_daily') }}
UNION ALL
SELECT * FROM {{ ref('new_payment_enabled_customer_cohort_daily')}}