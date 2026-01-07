
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cluster_size
from "test_idr"."main_idr_out"."identity_clusters"
where cluster_size is null



  
  
      
    ) dbt_internal_test