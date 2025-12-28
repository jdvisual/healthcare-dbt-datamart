{{ config(materialized='view') }}

select
  claim_id::number      as claim_id,
  member_id::number     as member_id,
  service_date::date    as service_date,
  upper(proc_code)      as proc_code
from {{ ref('claim_lines_demo') }}
