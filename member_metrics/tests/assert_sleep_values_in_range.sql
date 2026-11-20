-- Limits are our choice. Observed time in bed runs 60 to 953 minutes.
select *
from {{ ref('stg_pmdata__sleep_logs') }}
where time_in_bed_minutes not between 1 and 1440
   or minutes_asleep < 0
   or minutes_awake < 0
   or efficiency_pct not between 1 and 100
   or sleep_date not between '2019-10-01' and '2020-04-30'
