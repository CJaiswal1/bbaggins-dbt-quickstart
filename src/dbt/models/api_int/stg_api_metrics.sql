{{ config(materialized='incremental', schema='iops_bronze_sbx') }}

with src as (
  select
    t.*,
    farm_fingerprint(to_json_string(t)) as _row_hash
  from {{ source('iops_raw_sbx', 'raw_api_metrics') }} as t

  {% if is_incremental() %}
    where load_dw_audit_ts >= (
      select timestamp_sub(max(load_dw_audit_ts), interval 6 hour)
      from {{ this }}
    )
  {% endif %}
)

select src.* except(_row_hash)
from src

{% if is_incremental() %}
where not exists (
  select 1
  from {{ this }} as tgt
  where farm_fingerprint(to_json_string(tgt)) = src._row_hash
)
{% endif %}

