
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select source_id
from "test_idr"."main_idr_staging"."stg_identifiers"
where source_id is null



  
  
      
    ) dbt_internal_test