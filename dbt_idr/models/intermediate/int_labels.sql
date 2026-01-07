{{
    config(
        materialized='table',
        tags=['idr', 'intermediate']
    )
}}

{#
  Intermediate model: Label propagation to find connected components.
  
  Uses recursive CTE to propagate minimum labels through edges.
  Each entity gets assigned to the minimum entity_key in its connected component.
  
  Cross-platform compatible: Snowflake, BigQuery, Databricks, DuckDB
#}

{% if target.type == 'bigquery' %}
{# BigQuery requires different recursive CTE syntax #}

with all_nodes as (
    select distinct left_entity_key as entity_key from {{ ref('int_edges') }}
    union distinct
    select distinct right_entity_key as entity_key from {{ ref('int_edges') }}
),

undirected_edges as (
    select left_entity_key as src, right_entity_key as dst from {{ ref('int_edges') }}
    union all
    select right_entity_key as src, left_entity_key as dst from {{ ref('int_edges') }}
),

-- Non-recursive approach for BigQuery: iterative min propagation
label_pass_1 as (
    select 
        n.entity_key,
        least(n.entity_key, coalesce(min(e.dst), n.entity_key)) as label
    from all_nodes n
    left join undirected_edges e on e.src = n.entity_key
    group by n.entity_key
),

label_pass_2 as (
    select 
        l.entity_key,
        least(l.label, coalesce(min(l2.label), l.label)) as label
    from label_pass_1 l
    left join undirected_edges e on e.src = l.entity_key
    left join label_pass_1 l2 on l2.entity_key = e.dst
    group by l.entity_key, l.label
),

label_pass_3 as (
    select 
        l.entity_key,
        least(l.label, coalesce(min(l2.label), l.label)) as label
    from label_pass_2 l
    left join undirected_edges e on e.src = l.entity_key
    left join label_pass_2 l2 on l2.entity_key = e.dst
    group by l.entity_key, l.label
),

final_labels as (
    select entity_key, label from label_pass_3
)

{% else %}
{# Snowflake, Databricks, DuckDB - use recursive CTE #}

with recursive 

all_nodes as (
    select distinct left_entity_key as entity_key from {{ ref('int_edges') }}
    union
    select distinct right_entity_key as entity_key from {{ ref('int_edges') }}
),

undirected_edges as (
    select left_entity_key as src, right_entity_key as dst from {{ ref('int_edges') }}
    union
    select right_entity_key as src, left_entity_key as dst from {{ ref('int_edges') }}
),

label_propagation as (
    select 
        entity_key,
        entity_key as label,
        0 as iteration
    from all_nodes
    
    union all
    
    select 
        lp.entity_key,
        least(lp.label, e.dst) as label,
        lp.iteration + 1
    from label_propagation lp
    join undirected_edges e on e.src = lp.entity_key
    where lp.iteration < {{ var('idr_max_lp_iterations', 30) }}
      and e.dst < lp.label
),

final_labels as (
    select 
        entity_key,
        min(label) as label
    from label_propagation
    group by entity_key
)

{% endif %}

select 
    entity_key,
    label as resolved_id,
    current_timestamp as updated_at
from final_labels
