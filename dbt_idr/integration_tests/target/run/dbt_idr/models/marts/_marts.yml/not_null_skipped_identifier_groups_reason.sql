
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select reason
from "test_idr"."main_idr_out"."skipped_identifier_groups"
where reason is null



  
  
      
    ) dbt_internal_test