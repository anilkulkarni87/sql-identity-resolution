
  
    
    

    create  table
      "test_idr"."main_idr_out"."stage_metrics__dbt_tmp"
  
    as (
      



with stages as (
    select 1 as stage_order, 'stg_identifiers' as stage_name, 
           (select count(*) from "test_idr"."main_idr_staging"."stg_identifiers") as rows_out
    union all
    select 2, 'int_edges', 
           (select count(*) from "test_idr"."main_idr_work"."int_edges")
    union all
    select 3, 'int_labels', 
           (select count(*) from "test_idr"."main_idr_work"."int_labels")
    union all
    select 4, 'identity_membership', 
           (select count(*) from "test_idr"."main_idr_out"."identity_membership")
    union all
    select 5, 'identity_clusters', 
           (select count(*) from "test_idr"."main_idr_out"."identity_clusters")
    union all
    select 6, 'golden_profiles', 
           (select count(*) from "test_idr"."main_idr_out"."golden_profiles")
    union all
    select 7, 'skipped_identifier_groups', 
           (select count(*) from "test_idr"."main_idr_out"."skipped_identifier_groups")
)

select
    '5abd49b6-0f01-4bb1-a4ca-c0cdbb024e07' as run_id,
    stage_name,
    stage_order,
    rows_out,
    current_timestamp as recorded_at
from stages
order by stage_order
    );
  
  