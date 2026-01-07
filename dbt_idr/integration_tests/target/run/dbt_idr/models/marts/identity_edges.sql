
  
    
    

    create  table
      "test_idr"."main_idr_out"."identity_edges__dbt_tmp"
  
    as (
      



select
    'rule_' || identifier_type as rule_id,
    left_entity_key,
    right_entity_key,
    identifier_type,
    identifier_value_norm,
    created_at as first_seen_ts,
    created_at as last_seen_ts
from "test_idr"."main_idr_work"."int_edges"
    );
  
  