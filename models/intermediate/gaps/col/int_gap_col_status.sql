{{ config(materialized='view') }}

{# -------------------------------------------------------------------------
  int_gap_col_status
  Tri-state output:
    gap_flag = 0  -> qualified (eligible) but OPEN
    gap_flag = 1  -> CLOSED (has qualifying evidence)
    gap_flag = NULL -> NOT QUALIFIED (ineligible or excluded)

  NOTE: This is an engine scaffold. You will plug in:
   - your actual member spine (dim/enrollment)
   - your claims/procedure events table
   - your code lists (CPT/HCPCS/ICD10PCS/LOINC as applicable)
------------------------------------------------------------------------- #}

with params as (
    select
        /* Measurement period boundaries - keep simple for now */
        date_from_parts(year(current_date), 1, 1)  as period_start,
        date_from_parts(year(current_date), 12, 31) as period_end,

        /* Eligibility window (adjust per client/spec) */
        45 as min_age,
        75 as max_age,

        /* Lookback windows */
        10 as colonoscopy_lookback_years,
        5  as sigmoidoscopy_lookback_years,
        5  as ct_colonography_lookback_years,
        3  as stool_dna_lookback_years
),

/* -------------------------------------------------------------------------
   1) Member spine (replace with your real member eligibility/enrollment table)
------------------------------------------------------------------------- */
member_spine as (
    select
        m.member_id,
        m.birth_date,
        /* if you have continuous enrollment, plan_id, lob, etc. add here */
        current_date as as_of_date
    from {{ ref('stg_members') }} m
),

member_elig as (
    select
        ms.*,
        p.period_start,
        p.period_end,
        /* Age as of period_end */
        datediff('year', ms.birth_date, p.period_end)
          - iff(
              dateadd('year', datediff('year', ms.birth_date, p.period_end), ms.birth_date) > p.period_end,
              1, 0
            ) as age_at_period_end,

        /* Eligible age band */
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

/* -------------------------------------------------------------------------
   2) Exclusions (scaffold): CRC cancer history, total colectomy, hospice
   Replace these with real clinical flags derived from diagnoses/procedures.
------------------------------------------------------------------------- */
member_exclusions as (
    select
        me.member_id,

        /* TODO: derive from dx/proc sources */
        0 as has_crc_cancer_history,
        0 as has_total_colectomy,
        0 as in_hospice
    from member_elig me
),

/* -------------------------------------------------------------------------
   3) Screening events (procedure/event spine)
   Replace stg_claim_lines with your actual event table and columns.
------------------------------------------------------------------------- */
events as (
    select
        cl.member_id,
        cl.service_date::date as event_date,
        upper(cl.proc_code) as proc_code,
        /* optional: claim_id, line_id, place_of_service, etc. */
        cl.claim_id
    from {{ ref('stg_claim_lines') }} cl
),

/* -------------------------------------------------------------------------
   4) Scenario flags (plug in code lists later)
   For now: placeholder boolean logic. Swap to "proc_code in (select code ...)"
------------------------------------------------------------------------- */
scenario_hits as (
    select
        e.*,

        /* TODO: replace with real code sets */
        case when 1=0 then 1 else 0 end as is_colonoscopy,
        case when 1=0 then 1 else 0 end as is_sigmoidoscopy,
        case when 1=0 then 1 else 0 end as is_ct_colonography,
        case when 1=0 then 1 else 0 end as is_fit_fobt,
        case when 1=0 then 1 else 0 end as is_stool_dna
    from events e
),

/* -------------------------------------------------------------------------
   5) Apply lookback windows
------------------------------------------------------------------------- */
qualified_evidence as (
    select
        sh.member_id,
        sh.claim_id,
        sh.event_date,

        /* Determine which scenario this row qualifies for (by window) */
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
    where closure_reason is not null
),

/* -------------------------------------------------------------------------
   6) Choose “best” evidence per member (durable first; then most recent)
------------------------------------------------------------------------- */
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

/* -------------------------------------------------------------------------
   7) Final tri-state output
------------------------------------------------------------------------- */
final as (
    select
        me.member_id,
        'COL' as measure_id,
        year(me.period_end) as measurement_year,

        /* exclusion + eligibility */
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

        /* evidence */
        be.closure_date,
        be.closure_reason,
        be.evidence_id,

        /* optional: keep for debugging/audits */
        me.period_start,
        me.period_end,

        /* versioning */
        'COL_V1' as rule_version

    from member_elig me
    left join member_exclusions ex
        on me.member_id = ex.member_id
    left join best_evidence be
        on me.member_id = be.member_id
)

select * from final
 
