with aa as 
    (
select ar.subsidiary_name,  sum(count_location_last_month) as count_location_current_data_load
from ec_dw_stage.stage_revenue_activity_details rad
join ec_dw.account_revenue ar on rad.account_revenue_key = ar.account_revenue_key
where month_key = 20250301
and ar.flag_intercompany = 'customer'
group by 1
order by 2
    )
    
, bb as 
    (
select ar.subsidiary_name, sum(count_location_last_month) as count_location_prior_quarter
from ec_dw_stage.stage_revenue_activity_details_snapshot_lq rad
join ec_dw_stage.stage_account_revenue_snapshot_lq ar on rad.account_revenue_key = ar.account_revenue_key
where month_key = 20250301
and ar.flag_intercompany = 'customer'
group by 1
order by 2  
    )
    
    
    select aa.subsidiary_name
    , aa.count_location_current_data_load
    , bb.count_location_prior_quarter
    , aa.count_location_current_data_load - bb.count_location_prior_quarter as count_difference
    from aa
    join bb on aa.subsidiary_name = bb.subsidiary_name
order by 1


-- dig into a specific Solution Org 

with aa as
	(
select rad.subsidiary_key, account_revenue_id, account_revenue_name, sum(count_location_last_month) as count_account
from ec_dw_stage.stage_revenue_activity_details rad
join ec_dw.account_revenue ar on rad.account_revenue_key = ar.account_revenue_key
where month_key = 20250201
 and rad.subsidiary_key = 'pulsem'
 and ar.flag_intercompany = 'customer'
group by 1,2,3
order by 2
	)


, bb as
	(
select rad.subsidiary_key, account_revenue_id,  account_revenue_name, sum(count_location_last_month) as count_account
from ec_dw_stage.stage_revenue_activity_details_snapshot_lq rad
join ec_dw_stage.stage_account_revenue_snapshot_lq ar on rad.account_revenue_key = ar.account_revenue_key
where month_key = 20250201
 and rad.subsidiary_key = 'pulsem'
 and ar.flag_intercompany = 'customer'
group by 1,2,3
order by 2
	)

select coalesce(aa.subsidiary_key,bb.subsidiary_key) as subsidiary_key
, coalesce(aa.account_revenue_id,bb.account_revenue_id) as account_revenue_id
, coalesce(aa.account_revenue_name,bb.account_revenue_name) as account_revenue_name
, aa.count_account as count_account_current
, bb.count_account as count_account_last_quarter
, aa.count_account - bb.count_account
from aa
full outer join bb on aa.account_revenue_id = bb.account_revenue_id and aa.subsidiary_key = bb.subsidiary_key
where abs(coalesce(aa.count_account,0) - coalesce(bb.count_account,0)) > 0



select *
from ec_dw.account_revenue
where subsidiary_key = 'alertmd'
and account_revenue_id ilike '%Carle West Physician Group%'


210294

select *
from ec_dw_stage.stage_account_revenue_snapshot
where subsidiary_key = 'alertmd'
and account_revenue_id ilike '%Carle West Physician Group%'





select *
from ec_dw_upload.upload_revenue_monthly --_202409
where subsidiary_key = 'kickserv' 
-- and account_name ilike '%Bayside%'
and unique_identifier = 'cus_O8zjJGTKDAOvox'



select *
from ec_dw_upload.upload_revenue_monthly_202409
where subsidiary_key = 'kickserv' 
-- and account_name ilike '%Bayside%'
and unique_identifier = 'cus_O8zjJGTKDAOvox'


update ec_dw_upload.upload_revenue_monthly
set unique_identifier = 'Utahs Mighty Movers'
where unique_identifier = 'cus_O8zjJGTKDAOvox'
and subsidiary_key = 'kickserv' 

update ec_dw_upload.upload_revenue_monthly
set account_name = 'Utahs Mighty Movers'
where unique_identifier = 'cus_O8zjJGTKDAOvox' 
and subsidiary_key = 'kickserv' 


-------------- 

Determine if reactivations should be included 


select *
from ec_dw_stage.stage_account_revenue_snapshot_202409 sar
left join ec_dw.map_account_revenue_cancel_dates mar on sar.subsidiary_key = mar.subsidiary_key and sar.account_revenue_id = mar.account_revenue_id
where mar.account_revenue_id is null
and sar.month_cancel_software < '9/1/2024' -- do last month of previous quarter based on the last month of quarter is not included in map table
and sar.subsidiary_key not in ('joist','updox','invoicesimple','qiigo','listen360','goodtherapyorg','asf','clubos','mypthub','clubwise','thestudiodirector')
and sar.has_no_revenue = 0
and sar.account_revenue_id not ilike '%-ps'


