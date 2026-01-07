
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test: Verify confidence scores are calculated
-- All clusters should have confidence_score between 0 and 1

select 'FAIL: Invalid confidence score' as error
from "test_idr"."main_idr_out"."identity_clusters"
where confidence_score is null
   or confidence_score < 0
   or confidence_score > 1
  
  
      
    ) dbt_internal_test