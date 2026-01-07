
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select right_entity_key
from "test_idr"."main_idr_work"."int_edges"
where right_entity_key is null



  
  
      
    ) dbt_internal_test