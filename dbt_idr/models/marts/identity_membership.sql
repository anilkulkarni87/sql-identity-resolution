{{
    config(
        materialized='table',
        tags=['idr', 'marts']
    )
}}

{#
  Mart model: Identity membership mapping.
  
  Maps each entity_key to its resolved cluster ID.
  Includes singletons (entities with no edges get their own cluster).
#}

with labeled_entities as (
    select
        entity_key,
        resolved_id,
        updated_at
    from {{ ref('int_labels') }}
),

-- Get all entities from staging (for singletons)
all_entities as (
    select distinct entity_key
    from {{ ref('stg_identifiers') }}
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
