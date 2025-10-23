
-- make sure there are not beginning counts
-- make sure the lost column are negative values
-- select all blank columns to the right and delete them. 
    -- Highlight first blank column at end of file - crtl-shift over to the end of file horizontally - right click and delete

/*
Archinve the data 

drop table ec_dw_upload.archive_revenue_locations

select * into ec_dw_upload.archive_revenue_locations
from ec_dw_upload.upload_revenue_locations
order by "month" desc


*/

select *
from ec_dw_upload.upload_revenue_locations_backup_202501
where sub ilike '%invoice%'


delete from ec_dw_upload.upload_revenue_locations where sub ilike '%joist%'


insert into  ec_dw_upload.upload_revenue_locations
select * from ec_dw_upload.upload_revenue_locations_backup_202502_v3
where sub ilike '%invoice%'
order by month::date


select *
into ec_dw_upload.upload_revenue_locations_backup_202502_v4
from ec_dw_upload.upload_revenue_locations


select *
into ec_dw_upload.upload_revenue_locations_backup_202502
from ec_dw_upload.upload_revenue_locations



select count(*) from ec_dw_upload.upload_revenue_locations_backup

select  sub, max("month"::date)
from ec_dw_upload.upload_revenue_locations
group by 1



----------------------Updox --------------------------------
-- New Locations
select *
from ec_dw_upload.upload_revenue_locations
-- where sub = 'Updox'
order by "month"::date desc



insert into  ec_dw_upload.upload_revenue_locations
(sub,attribute1,"month",beginning,"new",lost)

select 'Updox','Direct', '3/1/2025','','24','-53'

insert into  ec_dw_upload.upload_revenue_locations
(sub,attribute1,"month",beginning,"new",lost)

select 'Updox','Indirect', '3/1/2025','','613', '-312'





----------------------------------Listen360 -----
-- Run from Eccel report in \EverCommerce\EC-PowerBI - Documents\Excel_Reports\L360\L360 Location.xlsx

select *
from ec_dw_upload.upload_revenue_locations
where sub = 'Listen360'
order by "month"::date desc


insert into  ec_dw_upload.upload_revenue_locations
(sub,attribute1,"month",beginning,"new",lost)

select 'Listen360','', '4/1/2025','','84','-81'


update ec_dw_upload.upload_revenue_locations
set "new" = '212', lost = '-65'
where sub = 'Listen360'
and "month" = '1/1/2025'


----------------------------------Invoice Simple -----
-- Run from Eccel report in \EverCommerce\EC-PowerBI - Documents\Excel_Reports\L360\L360 Location.xlsx
-- 

select *
from ec_dw_upload.upload_revenue_locations
where sub = 'invoicesimple'
order by "month"::date desc

select month_key, sum(count_location)
    from ec_dw.revenue_activity_details
        where subsidiary_key = 'invoicesimple' -- and month_key = 20250201
        group by 1
        order by 1 desc


insert into  ec_dw_upload.upload_revenue_locations
(sub,attribute1,"month",beginning,"new",lost)

select 'invoicesimple','', '4/1/2025','','14737','-13108'


update ec_dw_upload.upload_revenue_locations
set "new" = '14442', lost = '-13941'
where sub = 'invoicesimple'
and "month" = '2/1/2025'

Dec = +1 from Cancel
                          Feb + 5 from cancel


----------------------------------Joist Simple -----
-- Run from Eccel report in \EverCommerce\EC-PowerBI - Documents\Excel_Reports\L360\L360 Location.xlsx
-- =(I118-I117)-M118

select *
from ec_dw_upload.upload_revenue_locations
where sub ilike 'joist'
order by "month"::date desc

select month_key, sum(count_location)
    from ec_dw.revenue_activity_details
        where subsidiary_key = 'joist' -- and month_key = 20250201
        group by 1
        order by 1 desc


insert into  ec_dw_upload.upload_revenue_locations
(sub,attribute1,"month",beginning,"new",lost)

select 'Joist','', '4/1/2025','','4557','-3261'


update ec_dw_upload.upload_revenue_locations
set  lost = '-3241' -- "new" = '3975' --,
where sub ilike 'joist'
and "month" = '2/1/2025'



----------------------------------Good Therapy -----
-- Run from Eccel report in \EverCommerce\EC-PowerBI - Documents\Excel_Reports\L360\L360 Location.xlsx

select *
from ec_dw_upload.upload_revenue_locations
where sub = 'Good Therapy'
order by "month"::date desc


insert into  ec_dw_upload.upload_revenue_locations
(sub,attribute1,"month",beginning,"new",lost)

select 'Good Therapy','', '3/1/2025','','214','-539'




-------------
update ec_dw_upload.upload_revenue_locations
set "new" = 254, lost = 385
where sub = 'Good Therapy'
and "month" = '9/1/2024'

-----------------------------------------------------------------------------------------
update ec_dw_upload.upload_revenue_locations
set sub = 'Qiigo'
where sub = 'qiigo'




delete ec_dw_upload.upload_revenue_locations
where  sub = 'Qiigo' and "month" = '3/1/2024'







-------------------------------------------------------------------------------------------------------
--- NA - no longer doing it this way
delete from ec_dw_upload.upload_revenue_locations2


copy ec_dw_upload.upload_revenue_locations2
from 's3://ec-bi-import/Joist_location_refactor_202501.csv'
iam_role 'arn:aws:iam::529075595116:role/EcBiCrossAccountRole,arn:aws:iam::827128837566:role/EcBiImportRoleDataAcccount'
region 'us-west-2'
delimiter ',' 
EMPTYASNULL
IGNOREHEADER 1;


select *
from ec_dw_upload.upload_revenue_locations2
order by 1


select distinct sub from ec_dw_upload.upload_revenue_locations
where sub in (select distinct sub from ec_dw_upload.upload_revenue_locations2)


--fix the sub mapping
update ec_dw_upload.upload_revenue_locations2
set sub = 'Joist'
where sub = 'joist';



-- move the new data over -- after any fixes **************
delete from ec_dw_upload.upload_revenue_locations
where sub in 
(
    select distinct sub 
from ec_dw_upload.upload_revenue_locations2
)
-- and  "month"::date >= '1/1/2022';

-- add in the subs missing for the new month
insert into ec_dw_upload.upload_revenue_locations
select * from ec_dw_upload.upload_revenue_locations2;



insert into ec_dw_upload.upload_revenue_locations
select * from ec_dw_upload.upload_revenue_locations_backup_202412
where sub = 'Joist';

select *
into ec_dw_upload.upload_revenue_locations_backup_202502_v5
from ec_dw_upload.upload_revenue_locations

delete from ec_dw_upload.upload_revenue_locations
where month = '1/1/2025'



Select sub, count(*)
from  ec_dw_upload.upload_revenue_locations
Group by sub;

select * into  ec_dw_upload.upload_revenue_locations
from ec_dw_upload.upload_revenue_locations_backup_202501

------------------- in case blanks were loaded ----------------------------
delete from ec_dw_upload.upload_revenue_locations
where sub = ''
--- load individual month*** only if needed


---------------- manually update / insert data in locations ---------------
-- check data 
select sub, attribute1, month, "new", lost
from ec_dw_upload.upload_revenue_locations
order by month::date desc


delete from ec_dw_upload.upload_revenue_locations
where month = '1/1/2025'






