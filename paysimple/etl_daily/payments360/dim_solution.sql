/*
This is the solution dimension table, which defines the solutions used across analytics and reporting for payments 360. 
Each row represents a unique solution and includes metadata including the associated EverBrand, solution group, 
display sort order, and an active flag.
*/

{{ config(materialized='table', dist='even') }}
SELECT 1                       AS solution_id           -- Unique identifier for the solution
     , 'Service Fusion'        AS solution_display_name -- Display name for reporting
     , 'service fusion'        AS solution              -- Name of the solution or product
     , 'EverPro'               AS everbrand             -- Associated EverBrand
     , 'Home & Field Services' AS solution_group        -- Vertical the solution belongs to
     , 1                       AS sort_order            -- Controls display order in reports
     , TRUE                    AS is_active             -- Indicates if the solution is currently active
UNION ALL
SELECT 2 , 'Profit Rhino + Callahan Roach', 'profit rhino', 'EverPro', 'Home & Field Services', 2, TRUE
UNION ALL
SELECT 3 , 'Service Nation', 'service nation', 'EverPro', 'Home & Field Services', 3, TRUE
UNION ALL
SELECT 4 , 'EverPro Edge', 'everpro edge', 'EverPro', 'Home & Field Services', 4, TRUE
UNION ALL
SELECT 5 , 'Kickserv', 'kickserv', 'EverPro', 'Home & Field Services', 5, TRUE
UNION ALL
SELECT 6 , 'Briostack', 'briostack', 'EverPro', 'Home & Field Services', 6, TRUE
UNION ALL
SELECT 7 , 'DynaSCAPE', 'dynascape', 'EverPro', 'Home & Field Services', 7, TRUE
UNION ALL
SELECT 8 , 'Fieldpoint', 'fieldpoint', 'EverPro', 'Home & Field Services', 8, TRUE
UNION ALL
SELECT 9 , 'i360', 'i360', 'EverPro', 'Home & Field Services', 9, TRUE
UNION ALL
SELECT 10, 'MarketSharp', 'marketsharp', 'EverPro', 'Home & Field Services', 10, TRUE
UNION ALL
SELECT 11, 'RoofSnap', 'roofsnap', 'EverPro', 'Home & Field Services', 11, TRUE
UNION ALL
SELECT 12, 'Joist', 'joist', 'EverPro', 'Mobile Solutions', 12, TRUE
UNION ALL
SELECT 13, 'InvoiceSimple', 'invoicesimple', 'EverPro', 'Mobile Solutions', 13, TRUE
UNION ALL
SELECT 14, 'Bold Technologies', 'bold technologies', 'EverPro', 'Security & Alarm', 14, TRUE
UNION ALL
SELECT 15, 'Perennial', 'perennial', 'EverPro', 'Security & Alarm', 15, TRUE
UNION ALL
SELECT 16, 'SIMS', 'sims', 'EverPro', 'Security & Alarm', 16, TRUE
UNION ALL
SELECT 17, 'SGS', 'sgs', 'EverPro', 'Security & Alarm', 17, TRUE
UNION ALL
SELECT 18, 'SIS', 'sis', 'EverPro', 'Security & Alarm', 18, TRUE
UNION ALL
SELECT 19, 'Customer Lobby', 'customer lobby', 'EverPro', 'Customer Experience Solutions', 19, TRUE
UNION ALL
SELECT 20, 'PulseM', 'pulsem', 'EverPro', 'Customer Experience Solutions', 20, TRUE
UNION ALL
SELECT 21, 'GuildQuality', 'guildquality', 'EverPro', 'Customer Experience Solutions', 21, TRUE
UNION ALL
SELECT 22, 'Listen360', 'listen360', 'EverPro', 'Customer Experience Solutions', 22, TRUE
UNION ALL
SELECT 23, 'PaySimple', 'direct: paysimple', 'EverPro', 'Payments', 23, TRUE
UNION ALL
SELECT 24, 'PaySimple', 'integrated partner: zen planner', 'EverPro', 'Payments', 24, TRUE
UNION ALL
SELECT 25, 'PaySimple', 'integrated partner: third party', 'EverPro', 'Payments', 25, TRUE
UNION ALL
SELECT 26, 'Studio Director', 'studio director', 'EverPro', 'Payments', 26, TRUE
UNION ALL
SELECT 27, 'DrChrono', 'drchrono', 'EverHealth', 'Electronic Medical Records / Practice Management', 27, TRUE
UNION ALL
SELECT 28, 'CollaborateMD', 'collaboratemd', 'EverHealth', 'Electronic Medical Records / Practice Management', 28, TRUE
UNION ALL
SELECT 29, 'eProvider', 'eprovider', 'EverHealth', 'Electronic Medical Records / Practice Management', 29, TRUE
UNION ALL
SELECT 30, 'AllMeds', 'allmeds', 'EverHealth', 'Electronic Medical Records / Practice Management', 30, TRUE
UNION ALL
SELECT 31, 'iSalus', 'isalus', 'EverHealth', 'Electronic Medical Records / Practice Management', 31, TRUE
UNION ALL
SELECT 32, 'AlertMD', 'alertmd', 'EverHealth', 'Patient Engagement Solutions', 32, TRUE
UNION ALL
SELECT 33, 'MD Tech Solutions', 'md tech solutions', 'EverHealth', 'Patient Engagement Solutions', 33, TRUE
UNION ALL
SELECT 34, 'Updox', 'updox', 'EverHealth', 'Patient Engagement Solutions', 34, TRUE
UNION ALL
SELECT 35, 'EMHware', 'emhware', 'EverHealth', 'Behavioral Health', 35, TRUE
UNION ALL
SELECT 36, 'GoodTherapy', 'goodtherapy', 'EverHealth', 'Behavioral Health', 36, TRUE
UNION ALL
SELECT 37, 'Therapy Partner', 'therapy partner', 'EverHealth', 'Behavioral Health', 37, TRUE
UNION ALL
SELECT 38, 'Timely', 'timely', 'Wellness', 'Wellness', 38, TRUE
UNION ALL
SELECT 39, 'SalonBiz', 'salonbiz', 'Wellness', 'Wellness', 39, TRUE
UNION ALL
SELECT 40, 'Keyword Connects', 'keyword connects', 'Marketing Technology', 'EverConnect', 40, TRUE
UNION ALL
SELECT 41, '33Mile', '33mile', 'Marketing Technology', 'EverConnect',41, TRUE
UNION ALL
SELECT 42, 'RMDC', 'rmdc', 'Marketing Technology', 'EverConnect', 42, TRUE
UNION ALL
SELECT 43, 'Best Pick Reports', 'best pick reports', 'Marketing Technology', 'Directories', 43, TRUE
UNION ALL
SELECT 44, 'Five Star Rated', 'five star rated', 'Marketing Technology', 'Directories', 44, TRUE
UNION ALL
SELECT 45, 'Brighter Vision', 'brighter vision', 'Marketing Technology', 'Digital Marketing Services', 45, TRUE
UNION ALL
SELECT 46, 'Market Hardware', 'market hardware', 'Marketing Technology', 'Digital Marketing Services', 46, TRUE
UNION ALL
SELECT 47, 'Jimmy Marketing', 'jimmy marketing', 'Marketing Technology', 'Digital Marketing Services', 47, TRUE
UNION ALL
SELECT 48, 'Socius', 'socius', 'Marketing Technology', 'Digital Marketing Services', 48, TRUE
UNION ALL
SELECT 49, 'Qiigo', 'qiigo', 'Marketing Technology', 'Digital Marketing Services', 49, TRUE
