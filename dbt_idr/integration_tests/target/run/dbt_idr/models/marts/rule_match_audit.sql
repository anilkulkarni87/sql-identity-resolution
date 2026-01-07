
  
    
    

    create  table
      "test_idr"."main_idr_out"."rule_match_audit__dbt_tmp"
  
    as (
      



select
    'd1e3bbd7-90cb-4ff0-bdd2-4d94b1a6b9db' as run_id,
    'rule_' || identifier_type as rule_id,
    count(*) as edges_created,
    min(created_at) as started_at,
    max(created_at) as ended_at
from "test_idr"."main_idr_work"."int_edges"
group by identifier_type
    );
  
  