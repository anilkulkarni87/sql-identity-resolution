



with identifier_groups as (
    select
        identifier_type,
        identifier_value_norm,
        max_group_size,
        count(*) as group_size,
        min(entity_key) as sample_entity_key
    from "test_idr"."main_idr_staging"."stg_identifiers"
    where identifier_value_norm is not null
    group by identifier_type, identifier_value_norm, max_group_size
),

skipped_groups as (
    select
        'd1e3bbd7-90cb-4ff0-bdd2-4d94b1a6b9db' as run_id,
        identifier_type,
        identifier_value_norm,
        group_size,
        max_group_size as max_allowed,
        sample_entity_key,
        'EXCEEDED_MAX_GROUP_SIZE' as reason,
        current_timestamp as skipped_at
    from identifier_groups
    where group_size > max_group_size
)

select * from skipped_groups