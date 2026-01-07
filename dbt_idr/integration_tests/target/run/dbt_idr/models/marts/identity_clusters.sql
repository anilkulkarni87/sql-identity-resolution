
  
    
    

    create  table
      "test_idr"."main_idr_out"."identity_clusters__dbt_tmp"
  
    as (
      



with cluster_sizes as (
    select
        resolved_id,
        count(*) as cluster_size
    from "test_idr"."main_idr_out"."identity_membership"
    group by resolved_id
),

cluster_edge_stats as (
    select
        m.resolved_id,
        count(distinct e.identifier_type) as edge_diversity,
        count(*) as edge_count
    from "test_idr"."main_idr_out"."identity_membership" m
    inner join "test_idr"."main_idr_work"."int_edges" e
        on e.left_entity_key = m.entity_key 
        or e.right_entity_key = m.entity_key
    group by m.resolved_id
),

max_diversity as (
    select coalesce(max(edge_diversity), 1) as max_edge_diversity
    from cluster_edge_stats
),

clusters_with_confidence as (
    select
        cs.resolved_id,
        cs.cluster_size,
        
        -- Edge diversity (number of identifier types)
        coalesce(es.edge_diversity, 0) as edge_diversity,
        
        -- Match density (actual edges / max possible edges for cluster size)
        case 
            when cs.cluster_size <= 1 then 1.0
            else least(1.0, coalesce(es.edge_count, 0) * 1.0 / greatest(1, cs.cluster_size - 1))
        end as match_density,
        
        -- Confidence score: weighted combination
        case 
            when cs.cluster_size = 1 then 1.0
            else round(
                0.50 * (coalesce(es.edge_diversity, 0) * 1.0 / md.max_edge_diversity) +
                0.35 * least(1.0, coalesce(es.edge_count, 0) * 1.0 / greatest(1, cs.cluster_size - 1)) +
                0.15,
                3
            )
        end as confidence_score,
        
        -- Primary reason
        case 
            when cs.cluster_size = 1 then 'SINGLETON_NO_MATCH_REQUIRED'
            when coalesce(es.edge_diversity, 0) >= 2 then concat(cast(es.edge_diversity as TEXT), ' identifier types')
            else 'Single identifier type'
        end as primary_reason,
        
        current_timestamp as updated_at
        
    from cluster_sizes cs
    left join cluster_edge_stats es on es.resolved_id = cs.resolved_id
    cross join max_diversity md
)

select * from clusters_with_confidence
    );
  
  