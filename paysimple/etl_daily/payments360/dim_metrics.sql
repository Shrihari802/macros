/*
This is the Payments 360 metric table, which defines the core set of metrics used in the Payments 360 report. 
It includes both base and derived metrics across the categories Customer, Payments, Invoicing, Market, and Financials. 
Each metric is classified by category, subcategory, data type, and includes metadata to support standardized reporting and analysis.
*/

{{ config(materialized='table', dist='even') }}

SELECT 1               AS metric_id                 -- Unique identifier for the metric
     , 'new_customers' AS metric_name               -- System-friendly metric name (snake_case)
     , 'New Customers' AS metric_display_name       -- Report display name
     , 'Customer'      AS metric_category           -- High-level grouping for the metric
     , 'Acquisition'   AS metric_subcategory        -- More specific subcategory within the category
     , 'count'         AS data_type                 -- Data type (count, sum, percent, average)
     , NULL            AS format_string             -- Optional format string for display (e.g., %, $, etc.)
     , 1               AS sort_order                -- Sort order for display or grouping
     , TRUE            AS is_active                 -- Indicates if the metric is currently in use
     , FALSE           AS is_derived                -- TRUE if the metric is calculated from other metrics
UNION ALL 
SELECT 2, 'active_customers', 'Active Software Customers', 'Customer', 'Engagement', 'count', NULL, 2, TRUE, FALSE
UNION ALL 
SELECT 3, 'reactivated_customers', 'Reactivated Customers', 'Customer', 'Reactivation', 'count', NULL, 3, TRUE, FALSE
UNION ALL 
SELECT 4, 'total_customers', 'Total Customers', 'Customer', 'Engagement', 'count', NULL, 4, TRUE, FALSE
UNION ALL 
SELECT 5, 'new_payment_enabled_accounts', 'New Payment Enabled Accounts (New PEs)', 'Payments', 'Acquisition', 'count', NULL, 5, TRUE, FALSE
UNION ALL 
SELECT 6, 'new_pes_new_customer_cohort', 'New PEs - New Customer Cohort', 'Payments', 'Acquisition', 'count', NULL, 6, TRUE, TRUE
UNION ALL 
SELECT 7, 'new_pes_existing_customer_cohort', 'New PEs - Existing Customer Cohort', 'Payments', 'Acquisition', 'count', NULL, 7, TRUE, TRUE 
UNION ALL 
SELECT 8, 'total_payment_enabled_accounts', 'Total Payment Enabled Accounts (PEs)', 'Payments', 'Engagement', 'count', NULL, 8, TRUE, FALSE
UNION ALL 
SELECT 9, 'new_processing_merchants', 'New Processing Merchants (Activations)', 'Payments', 'Acquisition', 'count', NULL, 9, TRUE, FALSE
UNION ALL 
SELECT 10, 'total_processing_merchants', 'Total Processing Merchants', 'Payments', 'Engagement', 'count', NULL, 10, TRUE, FALSE
UNION ALL 
SELECT 11, 'total_invoice_volume', 'Total Invoice Volume (TIV)', 'Invoicing', 'Volume', 'sum', NULL, 11, TRUE, FALSE
UNION ALL 
SELECT 12, 'total_invoice_count', 'Total Invoice Count (TIC)', 'Invoicing', 'Volume', 'sum', NULL, 12, TRUE, FALSE
UNION ALL 
SELECT 13, 'total_collected_volume', 'Total Collected Volume (TCV)', 'Payments', 'Volume', 'sum', NULL, 13, TRUE, FALSE
UNION ALL 
SELECT 14, 'total_collected_volume_count', 'Total Collected Volume Count (TCC)', 'Payments', 'Volume', 'sum', NULL, 14, TRUE, FALSE
UNION ALL 
SELECT 15, 'total_processed_volume', 'Total Processed Volume (TPV)', 'Payments', 'Volume', 'sum', NULL, 15, TRUE, FALSE
UNION ALL 
SELECT 16, 'total_transactions', 'Total Transactions (Txn)', 'Payments', 'Volume', 'count', NULL, 16, TRUE, FALSE
UNION ALL 
SELECT 17, 'wallet_share_invoiced_volume', 'Wallet Share - Invoiced Volume', 'Invoicing', 'Share', 'percent', NULL, 17, TRUE, TRUE
UNION ALL 
SELECT 18, 'wallet_share_collected_volume', 'Wallet Share - Collected Volume', 'Payments', 'Share', 'percent', NULL, 18, TRUE, TRUE
UNION ALL 
SELECT 19, 'average_ticket', 'Ave Ticket (TPV / Txn)', 'Payments', 'Efficiency', 'percent', NULL, 19, TRUE, TRUE
UNION ALL 
SELECT 20, 'volume_per_processing_merchant', 'Volume per Processing Merchant (TPV / PM)', 'Payments', 'Efficiency', 'average', NULL, 20, TRUE, TRUE
UNION ALL 
SELECT 21, 'transactions_per_merchant', 'Transactions per Merchant (Txn / PM)', 'Payments', 'Efficiency', 'average', NULL, 21, TRUE, TRUE
UNION ALL 
SELECT 22, 'attach_rate_new_pes', 'Attach Rate (New PEs)', 'Payments', 'Adoption', 'percent', NULL, 22, TRUE, TRUE
UNION ALL 
SELECT 23, 'attach_rate_all_pes', 'Attach Rate (ALL PEs)', 'Payments', 'Adoption', 'percent', NULL, 23, TRUE, TRUE
UNION ALL 
SELECT 24, 'penetration_rate', 'Pentration Rate', 'Market', 'Adoption', 'percent', NULL, 24, TRUE, TRUE
UNION ALL 
SELECT 25, 'activation_rate', 'Activation Rate', 'Payments', 'Adoption', 'percent', NULL, 25, TRUE, TRUE
UNION ALL 
SELECT 26, 'activated_rate', 'Activated Rate', 'Payments', 'Adoption', 'percent', NULL, 26, TRUE, TRUE
UNION ALL 
SELECT 27, 'estimated_revenue', 'Estimate Revenue', 'Financial', 'Forecast', 'sum', NULL, 27, TRUE, FALSE
UNION ALL 
SELECT 33, 'customer_churn', 'Customer Churn', 'Customer', 'Churn', 'count', NULL, 28, TRUE, FALSE
UNION ALL 
SELECT 34, 'pe_churn', 'Payment Enabled Account Churn (PE Chrun)', 'Payments', 'Churn', 'count', NULL, 29, TRUE, FALSE
UNION ALL 
SELECT 35, 'inactive_pes', 'Inactive PEs', 'Payments', 'Churn', 'count', NULL, 30, TRUE, FALSE
