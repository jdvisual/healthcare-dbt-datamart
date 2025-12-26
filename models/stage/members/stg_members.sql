{{ config(materialized='view') }}

select
  1 as member_id,
  to_date('1970-01-01') as birth_date