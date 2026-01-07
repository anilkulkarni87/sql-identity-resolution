
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test: Verify golden profiles have email values
-- At least some profiles should have email_primary populated

with profiles_with_email as (
    select count(*) as cnt from "test_idr"."main_idr_out"."golden_profiles"
    where email_primary is not null
)
select 'FAIL: No golden profiles have email' as error
from profiles_with_email
where cnt = 0
  
  
      
    ) dbt_internal_test