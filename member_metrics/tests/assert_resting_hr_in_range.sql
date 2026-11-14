-- Returns real readings outside a plausible resting range. Observed data runs about 44 to 77.
-- The 30 to 120 limits are our choice. Any row returned makes the test fail.
select *
from {{ ref('stg_pmdata__resting_heart_rate') }}
where resting_hr_bpm is not null
  and resting_hr_bpm not between 30 and 120
