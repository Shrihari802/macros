-- Configuration for the model
{{ config(materialized='table', dist="even") }}

WITH
     leads AS ( SELECT
                      lead_date::DATE                         AS reporting_date
                    , lead_source
                    , sub_lead_source
                    , partner
                    , total_affiliated_partners
                    , SUM( distributed_count )                AS leads
                    , SUM( affiliated_count )                        AS total_associated_leads     
               FROM {{ ref('roofsnap_sfdc_360_partner_leads') }}
               WHERE lead_date::DATE >= '2020-01-01' AND COALESCE(lead_status,'null') <> 'Convert - No Oppty'
               GROUP BY 1,2,3,4,5
            ),

mqls AS ( SELECT
                      mql_date::DATE                         AS reporting_date
                    , lead_source
                    , sub_lead_source
                    , partner
                    , total_affiliated_partners
                    , SUM( distributed_count )                AS mqls
                    , SUM( affiliated_count )                        AS total_associated_mqls     
               FROM {{ ref('roofsnap_sfdc_360_partner_leads') }}
               WHERE mql_date::DATE >= '2020-01-01'
                    AND NOT ( COALESCE(lead_status,'null') IN ('Convert - No Oppty','Unqualified') )
               GROUP BY 1,2,3,4,5
            ),

sals AS ( SELECT
                      sal_date::DATE                         AS reporting_date
                    , lead_source
                    , sub_lead_source
                    , partner
                    , total_affiliated_partners
                    , SUM( distributed_count )         AS sals
                    , SUM( affiliated_count )                 AS total_associated_sals     
                    , SUM( distributed_amount )                AS new_opp_mrr
                    , SUM( amount )                            AS total_associated_new_opp_mrr
               FROM {{ ref('roofsnap_sfdc_360_partner_opps') }}
               WHERE type = 'New Business'   
                    AND sal_date::DATE >= '2020-01-01'
               GROUP BY 1,2,3,4,5
            ),

sqls AS ( SELECT
                      sql_date::DATE                         AS reporting_date
                    , lead_source
                    , sub_lead_source
                    , partner
                    , total_affiliated_partners
                    , SUM( distributed_count )         AS sqls
                    , SUM( affiliated_count )                 AS total_associated_sqls
               FROM {{ ref('roofsnap_sfdc_360_partner_opps') }}
               WHERE type = 'New Business' 
                  AND sql_date::DATE >= '2020-01-01'
               GROUP BY 1,2,3,4,5
            ),

closed_wons AS ( SELECT
                      sql_date::DATE                         AS reporting_date
                    , lead_source
                    , sub_lead_source
                    , partner
                    , total_affiliated_partners
                    , SUM( distributed_count )               AS closed_wons
                    , SUM( affiliated_count )                 AS total_associated_closed_wons
                    , SUM( distributed_amount )                AS closed_won_mrr
                    , SUM( amount )                            AS total_associated_closed_won_mrr
               FROM {{ ref('roofsnap_sfdc_360_partner_opps') }}
               WHERE type = 'New Business' 
                      AND close_date::DATE >= '2020-01-01'
                      AND LOWER( opportunity_status ) ~ 'closed won'
               GROUP BY 1,2,3,4,5
            )

SELECT
      reporting_date
    , lead_source                              AS lead_source
    , sub_lead_source                          AS sub_lead_source
    , partner
    , total_affiliated_partners
    , 'RoofSnap'                                AS solution
    , SUM(leads)                                       AS leads
    , SUM(mqls)                                        AS mqls
    , SUM(sals)                                        AS sals
    , SUM(sqls)                                        AS sqls
    , SUM(closed_wons)                                 AS closed_wons
    , SUM(new_opp_mrr)                                 AS new_opp_mrr
    , SUM(closed_won_mrr)                              AS closed_won_mrr
    , SUM(total_associated_leads)                      AS total_associated_leads
    , SUM(total_associated_mqls)                       AS total_associated_mqls
    , SUM(total_associated_sals)                       AS total_associated_sals
    , SUM(total_associated_sqls)                       AS total_associated_sqls
    , SUM(total_associated_closed_wons)                AS total_associated_closed_wons
    , SUM(total_associated_new_opp_mrr)                AS total_associated_new_opp_mrr
    , SUM(total_associated_closed_won_mrr)             AS total_associated_closed_won_mrr
FROM leads
FULL OUTER JOIN mqls 
    USING (reporting_date, lead_source, sub_lead_source, partner, total_affiliated_partners)
FULL OUTER JOIN sals 
    USING (reporting_date, lead_source, sub_lead_source, partner, total_affiliated_partners)
FULL OUTER JOIN sqls 
    USING (reporting_date, lead_source, sub_lead_source, partner, total_affiliated_partners)
FULL OUTER JOIN closed_wons 
    USING (reporting_date, lead_source, sub_lead_source, partner, total_affiliated_partners)
GROUP BY 1,2,3,4,5,6
