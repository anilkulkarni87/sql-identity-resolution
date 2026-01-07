









    
    
    
    
    






    
    
    
    
    
    
    
        
    
    
    
    
        
            
            
            
            
            
            
            
            
            
                
            
            
            
            
            
        
    
        
            
            
            
            
            
            
            
            
            
                
            
            
            
            
            
        
    
        
    
        
    
        
    
        
    

    
    
    
    
    
    
    
        
    
    
    
    
        
    
        
    
        
            
            
            
            
            
            
            
            
            
                
            
            
            
            
            
        
    
        
            
            
            
            
            
            
            
            
            
                
            
            
            
            
            
        
    
        
    
        
    

    
    
    
    
    
    
    
        
    
    
    
    
        
    
        
    
        
    
        
    
        
            
            
            
            
            
            
            
            
            
                
            
            
            
            
            
        
    
        
            
            
            
            
            
            
            
            
            
                
            
            
            
            
            
        
    



with raw_identifiers as (
    
        
select
    'crm' as source_id,
    concat('crm', ':', cast(customer_id as TEXT)) as entity_key,
    'EMAIL' as identifier_type,
    cast(email as TEXT) as identifier_value_raw,
    cast(lower(email) as TEXT) as identifier_value_norm,
    False as is_hashed,
    1 as priority,
    100 as max_group_size,
    current_timestamp as extracted_at
from main.sample_crm_customers
where email is not null
            
union all

select
    'crm' as source_id,
    concat('crm', ':', cast(customer_id as TEXT)) as entity_key,
    'PHONE' as identifier_type,
    cast(phone as TEXT) as identifier_value_raw,
    cast(phone as TEXT) as identifier_value_norm,
    False as is_hashed,
    2 as priority,
    100 as max_group_size,
    current_timestamp as extracted_at
from main.sample_crm_customers
where phone is not null
            
union all

select
    'orders' as source_id,
    concat('orders', ':', cast(order_id as TEXT)) as entity_key,
    'EMAIL' as identifier_type,
    cast(customer_email as TEXT) as identifier_value_raw,
    cast(lower(customer_email) as TEXT) as identifier_value_norm,
    False as is_hashed,
    1 as priority,
    100 as max_group_size,
    current_timestamp as extracted_at
from main.sample_orders
where customer_email is not null
            
union all

select
    'orders' as source_id,
    concat('orders', ':', cast(order_id as TEXT)) as entity_key,
    'PHONE' as identifier_type,
    cast(contact_phone as TEXT) as identifier_value_raw,
    cast(contact_phone as TEXT) as identifier_value_norm,
    False as is_hashed,
    2 as priority,
    100 as max_group_size,
    current_timestamp as extracted_at
from main.sample_orders
where contact_phone is not null
            
union all

select
    'pos' as source_id,
    concat('pos', ':', cast(transaction_id as TEXT)) as entity_key,
    'LOYALTY' as identifier_type,
    cast(loyalty_card_id as TEXT) as identifier_value_raw,
    cast(loyalty_card_id as TEXT) as identifier_value_norm,
    False as is_hashed,
    3 as priority,
    10 as max_group_size,
    current_timestamp as extracted_at
from main.sample_pos_transactions
where loyalty_card_id is not null
            
union all

select
    'pos' as source_id,
    concat('pos', ':', cast(transaction_id as TEXT)) as entity_key,
    'PHONE' as identifier_type,
    cast(contact_phone as TEXT) as identifier_value_raw,
    cast(contact_phone as TEXT) as identifier_value_norm,
    False as is_hashed,
    2 as priority,
    100 as max_group_size,
    current_timestamp as extracted_at
from main.sample_pos_transactions
where contact_phone is not null
            
    
),

exclusions as (
    select identifier_type, identifier_value_pattern, match_type
    from "test_idr"."main"."idr_exclusions"
),


filtered_identifiers as (
    select r.*
    from raw_identifiers r
    where not exists (
        select 1 from exclusions e
        where e.identifier_type = r.identifier_type
          and (
            (e.match_type = 'EXACT' and r.identifier_value_norm = e.identifier_value_pattern)
            or (e.match_type = 'LIKE' and r.identifier_value_norm like e.identifier_value_pattern)
          )
    )
)

select * from filtered_identifiers