
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select identifier_value_norm
from "test_idr"."main_idr_staging"."stg_identifiers"
where identifier_value_norm is null



  
  
      
    ) dbt_internal_test