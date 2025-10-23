-- Manual Steps ---------------------------
-- open the file and save as UTF8 .csv file type, with the name that you will use in copy statement below




**************** 

-- Load the data ---------------------------

delete from ec_dw_upload.upload_revenue_dynascape_temp


-- check the counts pre load
select count(*) from ec_dw_upload.upload_revenue_dynascape_temp


-- Load the data
copy ec_dw_upload.upload_revenue_dynascape_temp
from 's3://ec-bi-import/DynaScape_202503_MRR_ByProduct_upload.csv'
iam_role 'arn:aws:iam::529075595116:role/EcBiCrossAccountRole,arn:aws:iam::827128837566:role/EcBiImportRoleDataAcccount'
region 'us-west-2'
delimiter ',' 
EMPTYASNULL
IGNOREHEADER 1;

  -- get errors if any
 select starttime, * from stl_load_errors
 order by starttime desc




-- check the counts post load
select count(*) from ec_dw_upload.upload_revenue_dynascape_temp

select top 100 *
from ec_dw_upload.upload_revenue_dynascape_temp


-- process data -----------------------------

-- Check the data and note the start and end months
select (replace(month_id,'-','') || '01')::int, sum(revenue::numeric(12,2)), count(*)
from ec_dw_upload.upload_revenue_dynascape_temp
group by 1
order by 1 desc


-- check the formatting of the cancel dates -- potential issue with the product loading
select count(distinct cancel_date::date)
from ec_dw_upload.upload_revenue_dynascape_temp
order by 1


********************************************************************************
-- delete / insert into upload_revenue_monthly

delete from  ec_dw_upload.upload_revenue_monthly
where subsidiary_key = 'dynascape'
and month_key >= 20240301 -- put in the beginning month for the newly loaded data in the temp table


insert into ec_dw_upload.upload_revenue_monthly
(subsidiary_key, subsidiary, unique_identifier, netsuite_id
, account_name, attribute1, attribute2, cancel_date, revenue, month_key
, incremental_file_number, month_last_load,billing_frequency )

select
lower(urm.subsidiary) as subsidiary_key, urm.subsidiary
, urm.unique_identifier
, null as netsuite_id
, urm.account_name
, '', ''
, case 
    when gd.unique_identifier is not null then null
    else urm.cancel_date
    end 
  as cancel_date
, urm.revenue
, (replace(month_id,'-','') || '01')::int as month_key
, 999
, date_trunc('month',getdate())::date
, case 
        when urm.subscription_term_in_months = '12' then 'annual'
        when urm.subscription_term_in_months = '1' then 'month'
        when urm.subscription_term_in_months = '3' then 'quarter'
        else null
    end
from ec_dw_upload.upload_revenue_dynascape_temp urm
left join 
    (
    select distinct subsidiary, unique_identifier
    from ec_dw_upload.upload_revenue_dynascape_temp
    where cancel_date is null
    ) as gd -- for the customer load, ensure that all product level cancel dates are removed if any product has a blank cancel date
on urm.subsidiary = gd.subsidiary and urm.unique_identifier = gd.unique_identifier


********************************************************************************
-- delete / insert into upload_revenue_product

delete from ec_dw_upload.upload_revenue_product
where subsidiary_key = 'dynascape'
and month_key >= 20240301

-- check the delete
select month_key, count(*)
from ec_dw_upload.upload_revenue_product
where subsidiary_key = 'dynascape'
group by 1
order by 1 desc



insert into ec_dw_upload.upload_revenue_product
(subsidiary_key, subsidiary, unique_identifier, netsuite_id
, account_name, attribute1, attribute2, attribute3, product, cancel_date, revenue, month_key, billing_frequency )

select
'dynascape', 'dynascape'
, unique_identifier
, null as netsuite_id
, account_name
, attribute1, attribute2, attribute3
, product
, cancel_date
, revenue
, (replace(month_id,'-','') || '01')::int as month_key
, case
        when subscription_term_in_months = '12' then 'annual'
        when subscription_term_in_months = '1' then 'month'
        when subscription_term_in_months = '3' then 'quarter'
        else null
    end
as billing_frequency
from ec_dw_upload.upload_revenue_dynascape_temp urm


-- check the insert
select month_key, count(*)
from ec_dw_upload.upload_revenue_product
where subsidiary_key = 'dynascape'
group by 1
order by 1 desc

---------------  ** DONE ** -------------------------------------


-- Original Create Table Statement --
create table ec_dw_upload.upload_revenue_dynascape_temp
(
subsidiary varchar(256),
unique_identifier varchar(256),
netsuite_id varchar(256),
account_name varchar(256),
attribute1 varchar(256),
attribute2 varchar(256),
attribute3 varchar(256),
product varchar(256),
cancel_date varchar(256),
subscription_term_in_months varchar(256),
revenue varchar(256),
month_id varchar(256)
);


-- previous version of loading data

select top 100 * from ec_dw_upload.upload_revenue_monthly_dynascape

select count(*) from ec_dw_upload.upload_revenue_monthly_dynascape

