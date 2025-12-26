{{ config(materialized='incremental', unique_key='exception_key') }}

with src as (
    select * from {{ ref('int_gap_col_violations') }}
),

final as (
    select
        md5(
            coalesce(cast(member_id as varchar), '') || '|' ||
            coalesce(cast(source_measure_id as varchar), '') || '|' ||
            coalesce(cast(measurement_year as varchar), '') || '|' ||
            coalesce(cast(gap_flag as varchar), '') || '|' ||
            coalesce(rule_id, '') || '|' ||
            coalesce(violation_reason, '')
        ) as exception_key,

        exception_ts,
        rule_version,
        rule_id,
        measure_id,
        violation_reason,

        member_id,
        source_measure_id,
        measurement_year,
        gap_flag
    from src
)

select * from final
{% if is_incremental() %}
where exception_key not in (select exception_key from {{ this }})
{% endif %}