 {{ config(materialized='view') }}

{# -------------------------------------------------------------------------
  int_gap_col_status
  Tri-state output:
    gap_flag = 0  -> qualified (eligible) but OPEN
    gap_flag = 1  -> CLOSED (has qualifying evidence)
    gap_flag = NULL -> NOT QUALIFIED (ineligible or excluded)
------------------------------------------------------------------------- #}

with params as (
    select
        date_from_parts(year(current_date), 1, 1)   as period_start,
        date_from_parts(year(current_date), 12, 31) as period_end,

        45 as min_age,
        75 as max_age,

        10 as colonoscopy_lookback_years,
        5  as sigmoidoscopy_lookback_years,
        5  as ct_colonography_lookback_years,
        3  as stool_dna_lookback_years
),

member_spine as (
    select
        m.member_id,
        m.birth_date,
        current_date as as_of_date
    from {{ ref('stg_members') }} m
),

member_elig as (
    select
        ms.*,
        p.period_start,
        p.period_end,

        datediff('year', ms.birth_date, p.period_end)
          - iff(
              dateadd('year', datediff('year', ms.birth_date, p.period_end), ms.birth_date) > p.period_end,
              1, 0
            ) as age_at_period_end,

        case
            when (
                datediff('year', ms.birth_date, p.period_end)
                - iff(
                    dateadd('year', datediff('year', ms.birth_date, p.period_end), ms.birth_date) > p.period_end,
                    1, 0
                  )
            ) between p.min_age and p.max_age
            then 1 else 0
        end as is_age_eligible

    from member_spine ms
    cross join params p
),

member_exclusions as (
    select
        me.member_id,
        0 as has_crc_cancer_history,
        0 as has_total_colectomy,
        0 as in_hospice
    from member_elig me
),

events as (
    select
        cl.member_id,
        cl.service_date::date as event_date,
        upper(cl.proc_code) as proc_code,
        cl.claim_id
    from {{ ref('stg_claim_lines') }} cl
),

scenario_hits as (
    select
        e.*,

        /* demo: only FIT/FOBT wired for now */
        0 as is_colonoscopy,
        0 as is_sigmoidoscopy,
        0 as is_ct_colonography,

        case
          when exists (
            select 1
            from {{ ref('col_fit_fobt_codes') }} c
            where upper(c.code) = e.proc_code
          ) then 1 else 0
        end as is_fit_fobt,

        0 as is_stool_dna
    from events e
),

/* Build closure_reason in an inner select, then filter in the outer select */
qualified_evidence as (
    select
        q.member_id,
        q.claim_id,
        q.event_date,
        q.closure_reason
    from (
        select
            sh.member_id,
            sh.claim_id,
            sh.event_date,

            case
                when sh.is_colonoscopy = 1
                 and sh.event_date >= dateadd('year', -p.colonoscopy_lookback_years, p.period_end)
                 and sh.event_date <= p.period_end
                    then 'COLONOSCOPY'

                when sh.is_sigmoidoscopy = 1
                 and sh.event_date >= dateadd('year', -p.sigmoidoscopy_lookback_years, p.period_end)
                 and sh.event_date <= p.period_end
                    then 'SIGMOIDOSCOPY'

                when sh.is_ct_colonography = 1
                 and sh.event_date >= dateadd('year', -p.ct_colonography_lookback_years, p.period_end)
                 and sh.event_date <= p.period_end
                    then 'CT_COLONOGRAPHY'

                when sh.is_stool_dna = 1
                 and sh.event_date >= dateadd('year', -p.stool_dna_lookback_years, p.period_end)
                 and sh.event_date <= p.period_end
                    then 'STOOL_DNA_FIT'

                when sh.is_fit_fobt = 1
                 and sh.event_date >= p.period_start
                 and sh.event_date <= p.period_end
                    then 'FIT_FOBT'

                else null
            end as closure_reason

        from scenario_hits sh
        cross join params p
    ) q
    where q.closure_reason is not null
),

ranked_evidence as (
    select
        qe.*,
        case qe.closure_reason
            when 'COLONOSCOPY'      then 1
            when 'SIGMOIDOSCOPY'    then 2
            when 'CT_COLONOGRAPHY'  then 2
            when 'STOOL_DNA_FIT'    then 3
            when 'FIT_FOBT'         then 4
            else 99
        end as reason_rank,

        row_number() over (
            partition by qe.member_id
            order by
                case qe.closure_reason
                    when 'COLONOSCOPY'      then 1
                    when 'SIGMOIDOSCOPY'    then 2
                    when 'CT_COLONOGRAPHY'  then 2
                    when 'STOOL_DNA_FIT'    then 3
                    when 'FIT_FOBT'         then 4
                    else 99
                end asc,
                qe.event_date desc
        ) as rn
    from qualified_evidence qe
),

best_evidence as (
    select
        member_id,
        event_date as closure_date,
        closure_reason,
        claim_id as evidence_id
    from ranked_evidence
    where rn = 1
),

final as (
    select
        me.member_id,
        'COL' as measure_id,
        year(me.period_end) as measurement_year,

        me.is_age_eligible,
        ex.has_crc_cancer_history,
        ex.has_total_colectomy,
        ex.in_hospice,

        case
            when me.is_age_eligible = 0 then null
            when ex.has_crc_cancer_history = 1 then null
            when ex.has_total_colectomy = 1 then null
            when ex.in_hospice = 1 then null
            when be.member_id is not null then 1
            else 0
        end as gap_flag,

        be.closure_date,
        be.closure_reason,
        be.evidence_id,

        me.period_start,
        me.period_end,

        'COL_V1' as rule_version

    from member_elig me
    left join member_exclusions ex
        on me.member_id = ex.member_id
    left join best_evidence be
        on me.member_id = be.member_id
)

select * from final

