
 select starttime, * from stl_load_errors
 order by starttime desc




 
-- remove data from the previous load
delete from ec_dw_upload.upload_revenue_monthly_current_new

-- load the data from the .csv file uploaded to s3
copy ec_dw_upload.upload_revenue_monthly_current_new
from 's3://ec-bi-import/MRR.csv'
iam_role 'arn:aws:iam::529075595116:role/EcBiCrossAccountRole,arn:aws:iam::827128837566:role/EcBiImportRoleDataAcccount'
region 'us-west-2'
delimiter ',' 
EMPTYASNULL
IGNOREHEADER 1;


------------------
-- Check that ('Bold','DynaScape','Timely') have been deleted 


-- Check that the mapping will pick up the subsidiary_key ms.subsidiary_key field
-- Check that the min_month and max_month are consistent and correct to current month data load 
select  get_data.subsidiary,  ms.subsidiary_key
, min(key_date || '01')::integer as min_month
, max(key_date || '01')::integer as max_month
, count(*)
from ec_dw_upload.upload_revenue_monthly_current_new get_data
left join ec_dw_upload.upload_subsidiary ms on lower(replace(get_data.subsidiary,' ','')) = lower(replace(ms.map_ec_revenue_subsidiary,' ',''))
 -- where ms.subsidiary_key is null
group by 1,2
order by 2;


-- Check to ensure the Subsidiary in the file maps to the ms.subsidiary_key field
select  get_data.subsidiary,  ms.subsidiary_key, count(*)
from ec_dw_upload.upload_revenue_monthly_current_new get_data
left join ec_dw_upload.upload_subsidiary ms on lower(replace(get_data.subsidiary,' ','')) = lower(replace(ms.map_ec_revenue_subsidiary,' ',''))
   where ms.subsidiary_key is null
group by 1,2
order by 2;

---------- ONLY RUN IF AN ISSUE WITH ABOVE ms.subsidiary_key MAPPING ----------
-------------------------------------------------------------------------------
-- Run this to see the current Mapping which is the map_ec_revenue_subsidiary field
select distinct subsidiary_key, map_ec_revenue_subsidiary
from ec_dw_upload.upload_subsidiary
order by 1


--  UPDATE the map_ec_revenue_subsidiary field - and THEN rerun the above check of ms.subsidiary_key MAPPING
update ec_dw_upload.upload_subsidiary
set map_ec_revenue_subsidiary = 'AlertMD'
where subsidiary_key = 'alertmd'

---------- ** END ** ONLY RUN IF AN ISSUE WITH ABOVE ms.subsidiary_key MAPPING ----------
-------------------------------------------------------------------------------




----------------------------------------------------------------

-------------------------------------------------------------------------
--- CUSTOM UPDATES ---


-- perennialsoftware
update ec_dw_upload.upload_revenue_monthly_current_new
set unique_id = account_name || ' (' || unique_id || ')'
where subsidiary = 'Perennial'

--- ** END -- CUSTOM UPDATES ---



---------------CANCEL DATE CHECK for FILE CLEANUP-----------------------------------
-- check for bad cancel dates -- numbers instead of date format  -- IF any issues need to fix in file and reload

select  subsidiary, min(cancel_date), max(cancel_date)
from ec_dw_upload.upload_revenue_monthly_current_new
 where len(cancel_date) < 7
group by 1
order by 2

----------------------------------------------------------------
 -- CHECK Bill Frequency Assignments  -- IF any issues need to fix in file and reload

select distinct
case 
		when lower(coalesce(attribute_2,'month')) ilike 'every-other-month' then 2
		when lower(coalesce(attribute_2,'month')) ilike 'quarter' then 3
		when lower(coalesce(attribute_2,'month')) ilike 'quarterly' then 3
		when lower(coalesce(attribute_2,'month')) ilike 'semi-annual' then 6
		when lower(coalesce(attribute_2,'month')) ilike 'annual' then 12
		when lower(coalesce(attribute_2,'month')) ilike 'annually' then 12
		when lower(coalesce(attribute_2,'month')) ilike 'anually' then 12
		when lower(coalesce(attribute_2,'month')) ilike 'yearly' then 12
		when lower(coalesce(attribute_2,'month')) ilike '3year' then 36
		when lower(coalesce(attribute_2,'month')) ilike '3-Years' then 36	  else 1
	  end
	as months_term
    , attribute_2
 from ec_dw_upload.upload_revenue_monthly_current_new
  where subsidiary <> '33 Mile Radius'
 order by 1,2
 

--------------------------------------------------------
--- *** ----\
-- Ensure that the netsuite ID is NOT populated  -- only Studio Directory should have it

update ec_dw_upload.upload_revenue_monthly_current_new
set netsuite_id = null
where subsidiary <> 'Studio Director'

-----------------------------------------------------

-----------------------------------------------------------

-- FINAL truncate and load
delete from  ec_dw_upload.upload_revenue_monthly
where subsidiary_key in 
(
select distinct ms.subsidiary_key
from ec_dw_upload.upload_revenue_monthly_current_new get_data
left join ec_dw_upload.upload_subsidiary ms on lower(replace(get_data.subsidiary,' ','')) = lower(replace(ms.map_ec_revenue_subsidiary,' ',''))
)
and month_key >= 20240401


-----------insert ----------------------

insert into ec_dw_upload.upload_revenue_monthly
(subsidiary_key, subsidiary, unique_identifier, netsuite_id
, account_name, attribute1, attribute2, cancel_date, revenue, month_key
, incremental_file_number, month_last_load, billing_frequency)
select 
ms.subsidiary_key, mcn.subsidiary
, mcn.unique_id
, netsuite_id
, mcn.account_name
, mcn.attribute_1, mcn.attribute_2
, mcn.cancel_date
, mcn.revenue
, (mcn.key_date || '01')::integer as month_key
, 1
, date_trunc('month',getdate())::date
, case when subsidiary <> '33 Mile Radius' then attribute_2 else null end
from ec_dw_upload.upload_revenue_monthly_current_new mcn
left join ec_dw_upload.upload_subsidiary ms on lower(replace(mcn.subsidiary,' ','')) = lower(replace(ms.map_ec_revenue_subsidiary,' ',''));


-----------END LOAD ----------------------
-----------------------------------------------------
==========================================================================


---------- Small fixes to the DATA  ----------
------------------------------------------------------------------

---------------------------------------
-- First check / isolate data that needs Changing ----------------


select *
from ec_dw_upload.upload_revenue_monthly
where subsidiary_key = 'alertmd'
 -- and account_name ilike 'zzz%'
 -- and month_key > 20240301
and account_name = 'zzz -Nathaniel Ross MD'
order by month_key desc

-- Another Version of SELECT Statement to isolate data
select distinct subsidiary_key, account_name
from ec_dw_upload.upload_revenue_monthly
where  unique_identifier = '2444'
 --and unique_identifier in ('R657','')
-- and account_name ilike '%Mike Nichols%'
order by month_key desc, unique_identifier


---------- UPDATE the DATA  ----------
------------------------------------------------------------------
-- ADD the filters from SELECT to the following UPDATE
update ec_dw_upload.upload_revenue_monthly
set account_name = 'Nathaniel Ross MD' -- , unique_identifier = '*2516'
where subsidiary_key = 'alertmd'
and account_name = 'zzz -Nathaniel Ross MD'

and account_name = 'zzz-Practitioner Management Group Inc.'

and month_key = 20241201
and revenue = '1200.0'
and unique_identifier = '*1184'

-- ANOTHER VERSION OF UPDATE statement
update ec_dw_upload.upload_revenue_monthly
set unique_identifier = '1184'
where subsidiary_key = 'sims'
and unique_identifier = '*1184'




