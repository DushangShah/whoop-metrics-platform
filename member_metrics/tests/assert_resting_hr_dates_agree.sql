-- When both dates exist they must be the same day. Tests our reading of the inner date as MM/DD/YY.
select *
from {{ ref('stg_pmdata__resting_heart_rate') }}
where inner_date is not null
  and inner_date <> reading_date
