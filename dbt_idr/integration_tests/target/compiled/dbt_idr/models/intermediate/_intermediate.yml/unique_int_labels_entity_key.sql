
    
    

select
    entity_key as unique_field,
    count(*) as n_records

from "test_idr"."main_idr_work"."int_labels"
where entity_key is not null
group by entity_key
having count(*) > 1


