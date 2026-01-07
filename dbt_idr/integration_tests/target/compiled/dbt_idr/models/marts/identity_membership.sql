



with labeled_entities as (
    select
        entity_key,
        resolved_id,
        updated_at
    from "test_idr"."main_idr_work"."int_labels"
),

-- Get all entities from staging (for singletons)
all_entities as (
    select distinct entity_key
    from "test_idr"."main_idr_staging"."stg_identifiers"
),

-- Singletons: entities with no edges
singletons as (
    select
        e.entity_key,
        e.entity_key as resolved_id,  -- Singleton is its own cluster
        current_timestamp as updated_at
    from all_entities e
    where not exists (
        select 1 from labeled_entities l where l.entity_key = e.entity_key
    )
),

-- Combine labeled entities and singletons
final as (
    select entity_key, resolved_id, updated_at from labeled_entities
    union all
    select entity_key, resolved_id, updated_at from singletons
)

select * from final