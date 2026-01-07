
  
    
    

    create  table
      "test_idr"."main_idr_out"."run_history__dbt_tmp"
  
    as (
      



select
    'd1e3bbd7-90cb-4ff0-bdd2-4d94b1a6b9db' as run_id,
    cast('2026-01-07 04:07:28.704983+00:00' as timestamp) as started_at,
    current_timestamp as ended_at,
    (select count(*) from "test_idr"."main_idr_staging"."stg_identifiers") as identifiers_extracted,
    (select count(*) from "test_idr"."main_idr_work"."int_edges") as edges_created,
    (select count(*) from "test_idr"."main_idr_out"."identity_clusters") as clusters_total,
    (select count(*) from "test_idr"."main_idr_out"."identity_membership") as entities_resolved,
    (select count(*) from "test_idr"."main_idr_out"."identity_clusters" where cluster_size >= 100) as large_clusters,
    'SUCCESS' as status,
    'dev' as target_name,
    'duckdb' as target_type
    );
  
  