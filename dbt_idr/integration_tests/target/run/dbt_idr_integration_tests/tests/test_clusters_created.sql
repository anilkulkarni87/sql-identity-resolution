
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test: Verify clusters were created
-- Expected: Multiple clusters from matching identifiers

with cluster_count as (
    select count(*) as cnt from "test_idr"."main_idr_out"."identity_clusters"
)
select 'FAIL: No clusters created' as error
from cluster_count
where cnt = 0
  
  
      
    ) dbt_internal_test