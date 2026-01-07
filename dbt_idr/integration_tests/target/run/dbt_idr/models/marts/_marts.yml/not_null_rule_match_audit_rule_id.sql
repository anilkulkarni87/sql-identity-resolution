
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select rule_id
from "test_idr"."main_idr_out"."rule_match_audit"
where rule_id is null



  
  
      
    ) dbt_internal_test