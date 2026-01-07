
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    entity_key as unique_field,
    count(*) as n_records

from "test_idr"."main_idr_out"."identity_membership"
where entity_key is not null
group by entity_key
having count(*) > 1



  
  
      
    ) dbt_internal_test