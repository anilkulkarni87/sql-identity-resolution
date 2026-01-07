
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select resolved_id
from "test_idr"."main_idr_out"."golden_profiles"
where resolved_id is null



  
  
      
    ) dbt_internal_test