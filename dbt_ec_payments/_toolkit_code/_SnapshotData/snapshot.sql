1.) -- Snapshot the upload table
2.) -- Snapshot the previous month
3.) -- update the map cancel dates table


1.)    -------------*************** Snapshot the upload table ************************--------------------

-- put it into Archive ** -- change the YYYYMM
select * into ec_dw_upload.upload_revenue_monthly_202504 from ec_dw_upload.upload_revenue_monthly
    -- check it
select count(*) from ec_dw_upload.upload_revenue_monthly_202504;


2.) ------ **** --------------- Snapshot the previous month ----------------- **** ----------------------
-- Only run this when all the data has been loaded for the month, including paysimple
-- check and make sure that the data load ran successfully today

-- drop the snapshot tables
drop table ec_dw_stage.stage_account_revenue_snapshot;
drop table ec_dw_stage.stage_revenue_activity_details_snapshot;

-- create snapshot stages from the current data load
-- these stage tables will be picked up by dbt
select * into ec_dw_stage.stage_account_revenue_snapshot
from ec_dw.account_revenue;

select * into ec_dw_stage.stage_revenue_activity_details_snapshot
from ec_dw_stage.stage_revenue_activity_details


-------------------------------------------------------------------
-- put it into Archive ** -- change the YYYYMM
select * into ec_dw_stage.stage_account_revenue_snapshot_202504
from ec_dw.account_revenue;


select * into ec_dw_stage.stage_revenue_activity_details_snapshot_202504
from ec_dw_stage.stage_revenue_activity_details



-------------------------------------------------------------------
-- Quarter End Snapshots
-- this is a extra layer of exception reporting based on quarter end data was reported externally
-- ONLY RUN for Months 03, 06, 09, and 12
drop table ec_dw_stage.stage_account_revenue_snapshot_lq;
drop table ec_dw_stage.stage_revenue_activity_details_snapshot_lq;

select * into ec_dw_stage.stage_account_revenue_snapshot_lq
from ec_dw.account_revenue;

select * into ec_dw_stage.stage_revenue_activity_details_snapshot_lq
from ec_dw_stage.stage_revenue_activity_details



3.) -------------*************** update the map cancel dates table ************************--------------------

-- ALWAYS RUN THE SELECT BEFORE YOU RUN THE INSERT
-- Check that the date added is correct and that the month cancel software looks accurate
-----------------------------------------------------------------------
-- refresh the backup table
 drop table ec_dw.backup_map_account_revenue_cancel_dates

select * into ec_dw.backup_map_account_revenue_cancel_dates
from ec_dw.map_account_revenue_cancel_dates


--------- Map Cancel Dates ----------------------
insert into ec_dw.map_account_revenue_cancel_dates
(subsidiary_key,account_revenue_id,account_revenue_name,month_account_cancel_orig
,month_cancel_software,record_type,month_added)

select distinct
  sar.subsidiary_key
, sar.account_revenue_id
, sar.account_revenue_name
, sar.month_account_cancel::date as month_account_cancel_orig
, sar.month_cancel_software::date
, case when sar.month_cancel_software::date > dateadd(month,1,sar.month_account_cancel::date)::date then 'Canceled - Different Cancel Date Provided'
    else 'Canceled'
    end
as record_type
, dateadd(month,1,ru.max_date_month)::date as month_added -- change to the first month of the next quarter

from ec_dw_stage.source_account_revenue sar
left join ec_dw.map_account_revenue_cancel_dates mar on sar.subsidiary_key = mar.subsidiary_key and sar.account_revenue_id = mar.account_revenue_id
join (select subsidiary_key, max(date_month) as max_date_month from ec_dw_stage.source_revenue_upload group by 1) as ru on sar.subsidiary_key = ru.subsidiary_key
where mar.account_revenue_id is null -- not in the map table already
and sar.subsidiary_key not in ('joist','updox','invoicesimple','listen360','goodtherapyorg','asf','clubos','mypthub','clubwise','paysimple')
and sar.has_no_revenue = 0
and sar.account_revenue_id not ilike '%-ps'

and sar.month_cancel_software < dateadd(month,1,ru.max_date_month) -- based on the last month loaded per subsidiary



-------------*************** update the reactivations table ************************--------------------
 drop table ec_dw.backup_map_account_revenue_reactivations

select * into ec_dw.backup_map_account_revenue_reactivations
from ec_dw.map_account_revenue_reactivations

    select top 100 *
        from ec_dw.map_account_revenue_reactivations

insert into ec_dw.map_account_revenue_reactivations


 select distinct
	ar.subsidiary_key
	, ar.account_revenue_id || ' - Reactivated ' ||
    to_char(to_date(gr.month_key_first_revenue_reactivated::varchar,'YYYYMMDD'),'MM/YYYY')
    as account_revenue_id
	, ar.account_revenue_id as account_revenue_id_orig
	, ar.account_revenue_name
	, marcd.month_account_cancel_orig
	, marcd.month_cancel_software
	, arp.month_last_revenue as month_last_revenue_previous
  , gr.month_key_first_revenue_reactivated as month_key_first_revenue_reactivated
  , '4/1/2025'::date as month_added   -- change to current run month
-- into ec_dw.map_account_revenue_reactivations
from ec_dw.account_revenue ar
join ec_dw.map_account_revenue_cancel_dates marcd
on ar.subsidiary_key = marcd.subsidiary_key and ar.account_revenue_id = marcd.account_revenue_id
left join ec_dw_stage.stage_account_revenue_snapshot arp
on ar.subsidiary_key = arp.subsidiary_key and ar.account_revenue_id = arp.account_revenue_id
left join
(
 select distinct subsidiary_key, account_revenue_id_orig
 from ec_dw.map_account_revenue_reactivations
) as aa
on ar.subsidiary_key = aa.subsidiary_key and ar.account_revenue_id = aa.account_revenue_id_orig


 select ar.*
 from ec_dw.account_revenue ar
left join
(
 select distinct subsidiary_key, account_revenue_id
 from ec_dw.map_account_revenue_reactivations
) as arr
on ar.subsidiary_key = arr.subsidiary_key and ar.account_revenue_id = arr.account_revenue_id
 where arr.account_revenue_id is null
 and ar.account_revenue_id ilike '%Reactivated%'