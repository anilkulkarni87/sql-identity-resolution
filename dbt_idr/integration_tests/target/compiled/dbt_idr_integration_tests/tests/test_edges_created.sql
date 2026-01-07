-- Test: Verify edges were created
-- Should have edges from shared emails/phones

with edge_count as (
    select count(*) as cnt from "test_idr"."main_idr_out"."identity_edges"
)
select 'FAIL: No edges created' as error
from edge_count
where cnt = 0