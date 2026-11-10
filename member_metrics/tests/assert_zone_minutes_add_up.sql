-- total_minutes must equal the sum of the four zone columns.
select *
from {{ ref('stg_pmdata__zone_minutes') }}
where abs(total_minutes - (minutes_below_zone_1 + minutes_zone_1 + minutes_zone_2 + minutes_zone_3)) > 0.001
