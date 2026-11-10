-- The loaded data spans 2019-11-01 to 2020-03-31. The window below adds slack on both sides.
-- The limits are our choice, to catch dates that parsed but are obviously wrong.
select *
from {{ ref('stg_pmdata__zone_minutes') }}
where zone_date not between '2019-10-01' and '2020-04-30'
