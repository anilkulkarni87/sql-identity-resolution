
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select edges_created
from "test_idr"."main_idr_out"."rule_match_audit"
where edges_created is null



  
  
      
    ) dbt_internal_test