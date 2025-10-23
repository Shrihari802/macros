
truncate table ec_dw_upload.upload_grossmargin

copy ec_dw_upload.upload_grossmargin
from 's3://ec-bi-import/LTV.csv'
iam_role 'arn:aws:iam::529075595116:role/EcBiCrossAccountRole,arn:aws:iam::827128837566:role/EcBiImportRoleDataAcccount'
region 'us-west-2'
delimiter ','
EMPTYASNULL
IGNOREHEADER 1;

truncate table ec_dw_upload.upload_cac_everpro

copy ec_dw_upload.upload_cac_everpro
from 's3://ec-bi-import/upload_cac_everpro.csv'
iam_role 'arn:aws:iam::529075595116:role/EcBiCrossAccountRole,arn:aws:iam::827128837566:role/EcBiImportRoleDataAcccount'
region 'us-west-2'
delimiter ','
EMPTYASNULL
IGNOREHEADER 1;

truncate table ec_dw_upload.upload_cac_everhealth

copy ec_dw_upload.upload_cac_everhealth
from 's3://ec-bi-import/upload_cac_everhealth.csv'
iam_role 'arn:aws:iam::529075595116:role/EcBiCrossAccountRole,arn:aws:iam::827128837566:role/EcBiImportRoleDataAcccount'
region 'us-west-2'
delimiter ','
EMPTYASNULL
IGNOREHEADER 1;


truncate table ec_dw_upload.upload_cac_martech

copy ec_dw_upload.upload_cac_martech
from 's3://ec-bi-import/upload_cac_martech.csv'
iam_role 'arn:aws:iam::529075595116:role/EcBiCrossAccountRole,arn:aws:iam::827128837566:role/EcBiImportRoleDataAcccount'
region 'us-west-2'
delimiter ','
EMPTYASNULL
IGNOREHEADER 1;
