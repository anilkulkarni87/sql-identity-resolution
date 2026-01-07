
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select identifier_type
from "test_idr"."main_idr_out"."skipped_identifier_groups"
where identifier_type is null



  
  
      
    ) dbt_internal_test