-- A placeholder (heart rate 0) must come with no inner date, and a real reading must have one.
select *
from {{ ref('stg_pmdata__resting_heart_rate') }}
where (is_placeholder and inner_date is not null)
   or (not is_placeholder and inner_date is null)
