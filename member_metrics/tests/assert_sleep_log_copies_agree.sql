-- Repeated copies of a logId may differ in the 30-day averages and summary text (dropped in staging)
-- but must agree on every column staging keeps. Any row returned makes the test fail.
select participant_id, log_id
from {{ source('raw', 'raw_sleep_logs') }}
group by participant_id, log_id
having count(*) > 1
   and count(distinct hash(
        date_of_sleep, start_time_raw, end_time_raw, minutes_to_fall_asleep, minutes_asleep,
        minutes_awake, minutes_after_wakeup, time_in_bed, efficiency, sleep_type, info_code, main_sleep,
        deep_count, deep_minutes, light_count, light_minutes, rem_count, rem_minutes,
        wake_count, wake_minutes, asleep_count, asleep_minutes, restless_count, restless_minutes,
        awake_count, awake_minutes)) > 1
