 {{ config(materialized='view') }}

with v as (

    {{ gap_tri_state_violations(
        model_ref=ref('int_gap_col_status'),
        flag_col='gap_flag',
        pk_cols=['member_id','measure_id','measurement_year'],
        qualified_requires_cols=[],
        closed_requires_cols=['closure_date','closure_reason','evidence_id'],
        not_qualified_forbids_cols=['closure_date','closure_reason','evidence_id']
    ) }}

),

final as (
    select
        current_timestamp as exception_ts,
        'COL_V1' as rule_version,
        'COL_TRI_STATE' as rule_id,
        'COL' as measure_id,
        v.violation_reason,
        v.member_id,
        v.measure_id as source_measure_id,
        v.measurement_year,
        v.gap_flag
    from v
)

select * from final
