{% macro gap_tri_state_violations(
    model_ref,
    flag_col,
    pk_cols=[],
    qualified_requires_cols=[],
    closed_requires_cols=[],
    not_qualified_forbids_cols=[]
) %}

with base as (
    select *
    from {{ model_ref }}
),

a_flag_values as (
    select
        'A_FLAG_NOT_0_1_NULL' as violation_reason
        {%- for c in pk_cols -%}
        , {{ c }} as {{ c }}
        {%- endfor -%}
        , {{ flag_col }} as {{ flag_col }}
    from base
    where {{ flag_col }} is not null
      and {{ flag_col }} not in (0, 1)
)

{% if qualified_requires_cols | length > 0 %}
, b_qualified_missing as (
    select
        'B_QUALIFIED_MISSING_REQUIRED' as violation_reason
        {%- for c in pk_cols -%}
        , {{ c }} as {{ c }}
        {%- endfor -%}
        , {{ flag_col }} as {{ flag_col }}
    from base
    where {{ flag_col }} = 0
      and (
        {%- for c in qualified_requires_cols -%}
          {{ c }} is null
          {%- if not loop.last %} or {% endif -%}
        {%- endfor -%}
      )
)
{% endif %}

{% if closed_requires_cols | length > 0 %}
, c_closed_missing as (
    select
        'C_CLOSED_MISSING_REQUIRED' as violation_reason
        {%- for c in pk_cols -%}
        , {{ c }} as {{ c }}
        {%- endfor -%}
        , {{ flag_col }} as {{ flag_col }}
    from base
    where {{ flag_col }} = 1
      and (
        {%- for c in closed_requires_cols -%}
          {{ c }} is null
          {%- if not loop.last %} or {% endif -%}
        {%- endfor -%}
      )
)
{% endif %}

{% if not_qualified_forbids_cols | length > 0 %}
, d_not_qualified_has_metadata as (
    select
        'D_NOT_QUALIFIED_HAS_FORBIDDEN' as violation_reason
        {%- for c in pk_cols -%}
        , {{ c }} as {{ c }}
        {%- endfor -%}
        , {{ flag_col }} as {{ flag_col }}
    from base
    where {{ flag_col }} is null
      and (
        {%- for c in not_qualified_forbids_cols -%}
          {{ c }} is not null
          {%- if not loop.last %} or {% endif -%}
        {%- endfor -%}
      )
)
{% endif %}

select * from a_flag_values
{% if qualified_requires_cols | length > 0 %} union all select * from b_qualified_missing {% endif %}
{% if closed_requires_cols | length > 0 %} union all select * from c_closed_missing {% endif %}
{% if not_qualified_forbids_cols | length > 0 %} union all select * from d_not_qualified_has_metadata {% endif %}

{% endmacro %}