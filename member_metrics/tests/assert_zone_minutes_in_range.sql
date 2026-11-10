-- Returns days with impossible minute values. Any row returned makes the test fail.
select *
from {{ ref('stg_pmdata__zone_minutes') }}
where total_minutes < 0
   or total_minutes > 1440
   or minutes_below_zone_1 < 0
   or minutes_zone_1 < 0
   or minutes_zone_2 < 0
   or minutes_zone_3 < 0
