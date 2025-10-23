  -- get errors if any
 select starttime, * from stl_load_errors
 order by starttime desc

manual steps
-- remove the Billing System and Billing Plan fields
-- change the format on the numbers
-- change commas to pipes
-- save to csv



truncate table ec_dw_upload.upload_revenue_monthly_updox


copy ec_dw_upload.upload_revenue_monthly_updox
from 's3://ec-bi-import/Updox_upload_march2025.csv'
iam_role 'arn:aws:iam::529075595116:role/EcBiCrossAccountRole,arn:aws:iam::827128837566:role/EcBiImportRoleDataAcccount'
region 'us-west-2'
delimiter ',' 
EMPTYASNULL
IGNOREHEADER 1;


select count(*) from ec_dw_upload.upload_revenue_monthly_updox
-- 136509

update ec_dw_upload.upload_revenue_monthly_updox
set invoice_month = SPLIT_PART(invoice_month, '-', 2) || '/01/' || SPLIT_PART(invoice_month, '-', 1) 
where invoice_month ilike '%-%'

select distinct (to_char(date_trunc('month',invoice_month::date),'YYYYMM') || '01')::int
from ec_dw_upload.upload_revenue_monthly_updox
order by 1 desc







-- check data consistency
select distinct lower(billing_period) from ec_dw_upload.upload_revenue_monthly_updox

-- Check the date range of the data
select 
-- Check that the mapping will pick up the subsidiarykey
 min((to_char(date_trunc('month',invoice_month::date),'YYYYMM') || '01')::int) as min_month
, max((to_char(date_trunc('month',invoice_month::date),'YYYYMM') || '01')::int) as max_month
, count(*)
from ec_dw_upload.upload_revenue_monthly_updox get_data

-- determine counts per month as a check
select (to_char(date_trunc('month',invoice_month::date),'YYYYMM') || '01')::int
, count(*)
from  ec_dw_upload.upload_revenue_monthly_updox get_data
group by 1
order by 1 desc

select top 100 *
    from ec_dw_upload.upload_revenue_monthly_updox


-- load the data 
delete from ec_dw_upload.upload_revenue_monthly
where subsidiary_key = 'updox'
and month_key >= 20240101


insert into ec_dw_upload.upload_revenue_monthly
(subsidiary_key, subsidiary, unique_identifier, netsuite_id
, account_name, attribute1, attribute2, cancel_date, revenue, month_key
, incremental_file_number, month_last_load,billing_frequency)
select
'updox' as subsidiary_key, 'updox' as subsidiary
, unique_identifier, null as netsuite_id
, account_name
, null as attribute_1, null as attribute_2
, '1/1/2020' as cancel_date
, "amount"
, (to_char(date_trunc('month',invoice_month::date),'YYYYMM') || '01')::int as month_key
, 999
, date_trunc('month',getdate())::date
, lower(billing_period) as billing_frequency
from ec_dw_upload.upload_revenue_monthly_updox
where (to_char(date_trunc('month',invoice_month::date),'YYYYMM') || '01')::int >= 20240101;



select top 100 *
from ec_dw.account_revenue
where subsidiary_
where account_revenue_name ilike '%Aledade%'


select *
from ec_dw.

-- OLD CODE ---------------------------------------

update ec_dw_upload.upload_revenue_monthly_archive
set cancel_date = '1/1/2023'
where subsidiary_key = 'updox'


insert into ec_dw_upload.upload_revenue_monthly_archive
select * from ec_dw_upload.upload_revenue_monthly_archive_202404
where subsidiary_key = 'updox'

-- add the customer counts separately 
-- Source is the Updox Account by Month - Summary tab

insert into  ec_dw_upload.upload_revenue_locations
(sub,attribute1,"month",beginning,"new",lost)

select 'Updox','Direct', '7/1/2024','','41','-76'

insert into  ec_dw_upload.upload_revenue_locations
(sub,attribute1,"month",beginning,"new",lost)

select 'Updox','Indirect', '7/1/2024','','873', '-188'


select distinct sub
from ec_dw_upload.upload_revenue_locations
where sub <> 'Updox'
order by month::date desc



----------------------------------------------------------------------------------------------------
-- Archive load - not needed again --------------------------------------------------------------
insert into ec_dw_upload.upload_revenue_monthly_archive
(subsidiary_key, subsidiary, unique_identifier, netsuite_id
, account_name, attribute1, attribute2, cancel_date, revenue, month_key
,billing_frequency)

select
subsidiary_key, subsidiary, unique_identifier, netsuite_id
, account_name, attribute1, attribute2, cancel_date, revenue, month_key
,billing_frequency
from ec_dw_upload.upload_revenue_monthly_202312
where subsidiary_key = 'updox' and month_key = 20220101


select * 
from ec_dw_upload.upload_revenue_monthly_updox
where account_name ilike 'Southwest Surgical Associates'
and lower(billing_period) = 'semi-annual'
order by invoice_month::date desc


select distinct account_status
from ec_dw_upload.upload_revenue_monthly_updox



----------------------------------------
select * 
from paysimple.ps_dw.ec_revenue_paysimple
limit 100