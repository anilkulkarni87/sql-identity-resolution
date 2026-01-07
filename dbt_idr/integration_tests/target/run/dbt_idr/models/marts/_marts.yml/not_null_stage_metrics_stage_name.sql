
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select stage_name
from "test_idr"."main_idr_out"."stage_metrics"
where stage_name is null



  
  
      
    ) dbt_internal_test