
  
    
    

    create  table
      "test_idr"."main_idr_out"."golden_profiles__dbt_tmp"
  
    as (
      











    
    
    
    
    
    
    





    

    

    

    






    
    
    
    
    
    
    
        
    
    
    
    
    
        
            
        
    
        
            
        
    
        
            
        
    
        
            
        
    
        
            
        
    
        
    
        
    
        
    
        
    
        
    
    
    
    
    
    
    
    
    
    
    
        
        
    
        
        
    
        
        
    
        
        
    
    
    
    
    
    
    

    
    
    
    
    
    
    
        
    
    
    
    
    
        
    
        
    
        
    
        
    
        
    
        
            
        
    
        
            
        
    
        
            
        
    
        
    
        
    
    
    
    
    
    
    
    
    
    
    
        
        
    
        
        
    
        
        
    
        
        
    
    
    
    
    
    
    

    
    
    
    
    
    
    
        
    
    
    
    
    
        
    
        
    
        
    
        
    
        
    
        
    
        
    
        
    
        
            
        
    
        
            
        
    
    
    
    
    
    
    
    
    
    
    
        
        
    
        
        
    
        
        
    
        
        
    
    
    
    
    
    
    


with membership as (
    select distinct resolved_id, entity_key
    from "test_idr"."main_idr_out"."identity_membership"
),

entity_attributes as (
    
        
select 'crm' as source_id,
       concat('crm', ':', cast(customer_id as TEXT)) as entity_key,
       cast(email as TEXT) as email,
       cast(phone as TEXT) as phone,
       cast(first_name as TEXT) as first_name,
       cast(last_name as TEXT) as last_name,
       coalesce(cast(updated_at as timestamp), cast('1900-01-01' as timestamp)) as record_updated_at
from main.sample_crm_customers
    
union all

select 'orders' as source_id,
       concat('orders', ':', cast(order_id as TEXT)) as entity_key,
       cast(customer_email as TEXT) as email,
       cast(contact_phone as TEXT) as phone,
       cast(null as TEXT) as first_name,
       cast(null as TEXT) as last_name,
       coalesce(cast(order_date as timestamp), cast('1900-01-01' as timestamp)) as record_updated_at
from main.sample_orders
    
union all

select 'pos' as source_id,
       concat('pos', ':', cast(transaction_id as TEXT)) as entity_key,
       cast(null as TEXT) as email,
       cast(contact_phone as TEXT) as phone,
       cast(null as TEXT) as first_name,
       cast(null as TEXT) as last_name,
       coalesce(cast(transaction_date as timestamp), cast('1900-01-01' as timestamp)) as record_updated_at
from main.sample_pos_transactions
    
    
),

cluster_entities as (
    select
        m.resolved_id,
        e.*
    from membership m
    left join entity_attributes e on e.entity_key = m.entity_key
)



, email_ranked as (
    select resolved_id, email,
           row_number() over (partition by resolved_id order by record_updated_at desc) as rn
    from cluster_entities
    where email is not null
)

, phone_ranked as (
    select resolved_id, phone,
           row_number() over (partition by resolved_id order by record_updated_at desc) as rn
    from cluster_entities
    where phone is not null
)

, first_name_ranked as (
    select resolved_id, first_name,
           row_number() over (partition by resolved_id order by record_updated_at desc) as rn
    from cluster_entities
    where first_name is not null
)

, last_name_ranked as (
    select resolved_id, last_name,
           row_number() over (partition by resolved_id order by record_updated_at desc) as rn
    from cluster_entities
    where last_name is not null
)



, golden_profiles as (
    select distinct
        m.resolved_id
        
        , email_r.email as email_primary
        
        , phone_r.phone as phone_primary
        
        , first_name_r.first_name as first_name_primary
        
        , last_name_r.last_name as last_name_primary
        
        , current_timestamp as updated_at
    from membership m
    
    left join email_ranked email_r 
        on email_r.resolved_id = m.resolved_id and email_r.rn = 1
    
    left join phone_ranked phone_r 
        on phone_r.resolved_id = m.resolved_id and phone_r.rn = 1
    
    left join first_name_ranked first_name_r 
        on first_name_r.resolved_id = m.resolved_id and first_name_r.rn = 1
    
    left join last_name_ranked last_name_r 
        on last_name_r.resolved_id = m.resolved_id and last_name_r.rn = 1
    
)

select * from golden_profiles
    );
  
  