## Manual Step - 
1.) check that Billing Frequency is all Monthly
2.) remove the Billing Frequency
3.) Use the Python code to upload to MRR

4.) Change file name to starts with Timely_202403_MRR
5.) Change sn == Rev by Customer - Split Payment




delete from ec_dw_upload.upload_revenue_monthly_current_new

-- ensure that the subsidiary_key is populated correctly

copy ec_dw_upload.upload_revenue_monthly_current_new
from 's3://ec-bi-import/MRR.csv'
iam_role 'arn:aws:iam::529075595116:role/EcBiCrossAccountRole,arn:aws:iam::827128837566:role/EcBiImportRoleDataAcccount'
region 'us-west-2'
delimiter ',' 
EMPTYASNULL
IGNOREHEADER 1;


select subsidiary, subsidiary_key
-- Check that the mapping will pick up the subsidiarykey
, min(key_date || '01')::integer as min_month
, max(key_date || '01')::integer as max_month
, count(*)
from ec_dw_upload.upload_revenue_monthly_current_new get_data
left join ec_dw_upload.upload_subsidiary ms on lower(replace(get_data.subsidiary,' ','')) = lower(replace(ms.map_ec_revenue_subsidiary,' ',''))
 -- where ms.subsidiary_key is null
group by 1,2
order by 2;


-- check for bad dates
select  subsidiary, min(cancel_date), max(cancel_date)
from ec_dw_upload.upload_revenue_monthly_current_new
where len(cancel_date) < 7
group by 1
order by 2

update ec_dw_upload.upload_revenue_monthly_current_new
set cancel_date = null
where cancel_date ilike 'nat'


update ec_dw_upload.upload_revenue_monthly_current_new
set revenue = (revenue::numeric(12,2)*.6772)::varchar
where subsidiary ilike 'Timely'



----- adjust the Revenue to USD

-- ****************************
-- product data load
-- *******************************
delete from ec_dw_upload.upload_revenue_product
where subsidiary_key = 'timely'
and month_key >= 20240301



---- check that the delete worked
select subsidiary_key, month_key, count(*)
from ec_dw_upload.upload_revenue_product
where subsidiary_key = 'timely'
group by 1,2
order by 1,2

--- PRODUCT LOADS UPDATES ________________________________________________


----------------------------------------------------------------------------------------
insert into ec_dw_upload.upload_revenue_product
(subsidiary_key, subsidiary, unique_identifier, netsuite_id
, account_name, attribute1, attribute2, attribute3, product, cancel_date, revenue, month_key )
select
'timely', 'timely'
, mcn.unique_id
, null as netsuite_id
, mcn.account_name
, mcn.attribute_1
, '' as attribute2, '' as attribute3
, mcn.attribute_2 as product
, mcn.cancel_date
, mcn.revenue
, (mcn.key_date || '01')::integer as month_key
from ec_dw_upload.upload_revenue_monthly_current_new mcn
where subsidiary ilike 'timely';




-----------------------------------------------------
-- LOAD REVENUE
-----------------------------------------------------


-- final truncate and load
delete from  ec_dw_upload.upload_revenue_monthly
where subsidiary_key in 
(
select distinct ms.subsidiary_key
from ec_dw_upload.upload_revenue_monthly_current_new get_data
left join ec_dw_upload.upload_subsidiary ms on lower(replace(get_data.subsidiary,' ','')) = lower(replace(ms.map_ec_revenue_subsidiary,' ',''))
)
and month_key >= 20240301



insert into ec_dw_upload.upload_revenue_monthly
(subsidiary_key, subsidiary, unique_identifier, netsuite_id
, account_name, attribute1, attribute2, cancel_date, revenue, month_key
, incremental_file_number, month_last_load )
select 
ms.subsidiary_key, mcn.subsidiary
, mcn.unique_id
, null as netsuite_id
, mcn.account_name
, mcn.attribute_1, mcn.attribute_2
, mcn.cancel_date
, mcn.revenue
, (mcn.key_date || '01')::integer as month_key
, 1
, date_trunc('month',getdate())::date 
from ec_dw_upload.upload_revenue_monthly_current_new mcn
left join ec_dw_upload.upload_subsidiary ms on lower(replace(mcn.subsidiary,' ','')) = lower(replace(ms.map_ec_revenue_subsidiary,' ',''));


select month_key, sum(revenue)
from ec_dw_upload.upload_revenue_monthly
where subsidiary_key = 'timely'
group by 1
order by 1 desc
