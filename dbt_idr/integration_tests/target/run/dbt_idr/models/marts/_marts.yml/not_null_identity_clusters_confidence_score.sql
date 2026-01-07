
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select confidence_score
from "test_idr"."main_idr_out"."identity_clusters"
where confidence_score is null



  
  
      
    ) dbt_internal_test