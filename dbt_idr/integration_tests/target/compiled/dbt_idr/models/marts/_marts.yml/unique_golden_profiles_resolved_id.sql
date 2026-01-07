
    
    

select
    resolved_id as unique_field,
    count(*) as n_records

from "test_idr"."main_idr_out"."golden_profiles"
where resolved_id is not null
group by resolved_id
having count(*) > 1


