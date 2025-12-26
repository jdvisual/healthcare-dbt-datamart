{{ config(materialized='view') }}

select
  1 as member_id,
  to_date('2025-01-15') as service_date,
  'TEST' as proc_code,
  'CLAIM1' as claim_id