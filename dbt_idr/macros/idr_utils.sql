{#
  Normalize an identifier value based on the rule's canonicalize setting.
  
  Usage: {{ normalize_identifier('email_value', 'LOWERCASE') }}
#}

{% macro normalize_identifier(value_expr, canonicalize_type) %}
    case 
        when '{{ canonicalize_type }}' = 'LOWERCASE' then lower({{ value_expr }})
        when '{{ canonicalize_type }}' = 'UPPERCASE' then upper({{ value_expr }})
        else {{ value_expr }}
    end
{% endmacro %}


{#
  Generate a composite entity key from source_id and raw key.
  
  Usage: {{ generate_entity_key('crm_customers', 'customer_id') }}
#}

{% macro generate_entity_key(source_id, key_column) %}
    concat('{{ source_id }}', ':', cast({{ key_column }} as {{ dbt.type_string() }}))
{% endmacro %}


{#
  Check if an identifier value should be excluded based on exclusion rules.
  Returns true if the value should be KEPT (not excluded).
  
  Usage: {{ is_valid_identifier('identifier_type', 'identifier_value') }}
#}

{% macro is_valid_identifier(type_column, value_column) %}
    not exists (
        select 1 
        from {{ ref('idr_exclusions') }} e
        where e.identifier_type = {{ type_column }}
          and (
            (e.match_type = 'EXACT' and {{ value_column }} = e.identifier_value_pattern)
            or (e.match_type = 'LIKE' and {{ value_column }} like e.identifier_value_pattern)
          )
    )
{% endmacro %}
