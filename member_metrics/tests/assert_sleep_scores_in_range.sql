-- Returns rows with impossible sleep values. Any row returned makes the test fail.
select *
from {{ ref('stg_pmdata__sleep_score') }}
where overall_score not between 0 and 100
   or deep_sleep_minutes not between 0 and 600
   or resting_heart_rate_bpm not between 30 and 120
   or restlessness not between 0 and 1
