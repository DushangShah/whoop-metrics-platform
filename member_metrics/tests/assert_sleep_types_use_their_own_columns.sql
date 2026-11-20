-- stages records fill deep/light/rem/wake and leave the classic columns empty, and the reverse.
select *
from {{ ref('stg_pmdata__sleep_logs') }}
where sleep_type is null
   or sleep_type not in ('stages', 'classic')
   or (sleep_type = 'stages' and (
          classic_asleep_minutes is not null or deep_minutes is null or light_minutes is null
          or rem_minutes is null or wake_minutes is null))
   or (sleep_type = 'classic' and (
          deep_minutes is not null or classic_asleep_minutes is null
          or classic_restless_minutes is null or classic_awake_minutes is null))
