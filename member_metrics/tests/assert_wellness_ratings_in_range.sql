-- Returns rows with ratings outside their documented scale. Any row returned makes the test fail.
select *
from {{ ref('stg_pmdata__wellness') }}
where fatigue not between 1 and 5
   or mood not between 1 and 5
   or sleep_quality not between 1 and 5
   or soreness not between 1 and 5
   or stress not between 1 and 5
   or readiness not between 0 and 10
   or sleep_duration_hours not between 0 and 24
