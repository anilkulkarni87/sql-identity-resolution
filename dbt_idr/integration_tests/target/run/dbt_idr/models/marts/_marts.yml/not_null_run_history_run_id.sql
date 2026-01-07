
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select run_id
from "test_idr"."main_idr_out"."run_history"
where run_id is null



  
  
      
    ) dbt_internal_test