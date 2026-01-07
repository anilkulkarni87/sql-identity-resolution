
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select status
from "test_idr"."main_idr_out"."run_history"
where status is null



  
  
      
    ) dbt_internal_test