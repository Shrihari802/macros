{% macro ec_mkt_clean_partner(column_ref) %}

 CASE
        WHEN {{ column_ref }} IN ('Authority Brands Vendor Showcase') 
            THEN 'Authority Brands'
        WHEN {{ column_ref }} IN ('Authority Brands/BuyMax', 'Buy Max', 'BuyMax', 'BuyMax/Authority Brands') 
            THEN 'Authority Brands - BuyMax'
        WHEN {{ column_ref }} IN ('BCI (Bath Planet, Luxury Bath, Prime Bath, Bath Makeover)', 'BCI Acrylic', 'BCI White Label') 
            THEN 'BCI'
        WHEN {{ column_ref }} IN ('C.H.I. Overhead Doors', 'CHI') 
            THEN 'C.H.I.'
        WHEN {{ column_ref }} IN ('Certain Path', 'Certain Path / SGI', 'SGI/CertainPath') 
            THEN 'CertainPath'
        WHEN {{ column_ref }} IN ('Daxko Spectrum(CSI)') 
            THEN 'Daxko'
        WHEN {{ column_ref }} IN ('FPMA: Florida Pest Management Association') 
            THEN 'FPMA'
        WHEN {{ column_ref }} IN ('Home Franchise Concepts Expo') 
            THEN 'Home Franchise Concepts'
        WHEN {{ column_ref }} IN ('IBPSA: International Boarding & Pet Services Association') 
            THEN 'IBPSA'
        WHEN {{ column_ref }} IN ('IDA: International Door Association') 
            THEN 'IDA'
        WHEN {{ column_ref }} IN ('Lansing Building Products') 
            THEN 'Lansing BP'
        WHEN {{ column_ref }} IN ('NALP: National Association of Landscape Professionals') 
            THEN 'NALP'
        WHEN {{ column_ref }} IN ('NCSG: National Chimney Sweep Guild') 
            THEN 'NCSG'
        WHEN {{ column_ref }} IN ('Neighborly Reunion') 
            THEN 'Neighborly'
        WHEN {{ column_ref }} IN ('Mr. Electric') 
            THEN 'Neighborly - Mr. Electric'
        WHEN {{ column_ref }} IN ('Mr. Handyman') 
            THEN 'Neighborly - Mr. Handyman'
        WHEN {{ column_ref }} IN ('Nexstar Super Meeting', 'Nextar', 'Nextstar') 
            THEN 'Nexstar'
        WHEN {{ column_ref }} IN ('PCOC: Pest Control Operators of California') 
            THEN 'PCOC'
        WHEN {{ column_ref }} IN ('PHCC Connect', 'PHCC: Plumbing, Heating, Cooling Contractors Association') 
            THEN 'PHCC'
        WHEN {{ column_ref }} IN ('Re-Bath') 
            THEN 'ReBath'
        WHEN {{ column_ref }} IN ('ServiceNation') 
            THEN 'Service Nation'
        WHEN {{ column_ref }} IN ('ServiceTitan') 
            THEN 'Service Titan'
        WHEN {{ column_ref }} IN ('SGI Fall Expo', 'SGI Spring Expo') 
            THEN 'SGI'
        WHEN {{ column_ref }} IN ('Sherwin Williams - MetalVue') 
            THEN 'Sherwin-Williams'
        WHEN {{ column_ref }} IN ('Sierra Pacific Windows') 
            THEN 'Sierra Pacific'
        WHEN {{ column_ref }} IN ('TCIA: Tree Care Industry Association') 
            THEN 'TCIA'
        WHEN {{ column_ref }} IN ('VGM Heartland') 
            THEN 'VGM'
        WHEN {{ column_ref }} LIKE '% (OP)'
            THEN REPLACE( {{ column_ref }}, ' (OP)', '')
        WHEN {{ column_ref }} LIKE '% OR'
            THEN REPLACE( {{ column_ref }}, ' OR', '')
        ELSE TRIM( {{ column_ref }} )
    END
    
{% endmacro %}




