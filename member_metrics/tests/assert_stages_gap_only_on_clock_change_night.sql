-- For stages records, time in bed equals asleep + awake except on 2020-03-29, where the gap is 0 or 60.
-- Tests our reading that the gap comes from a clock change. Fails if the gap shows up anywhere else.
select *
from {{ ref('stg_pmdata__sleep_logs') }}
where sleep_type = 'stages'
  and (
      (sleep_date <> '2020-03-29' and time_in_bed_gap_minutes <> 0)
      or (sleep_date = '2020-03-29' and time_in_bed_gap_minutes not in (0, 60))
  )
