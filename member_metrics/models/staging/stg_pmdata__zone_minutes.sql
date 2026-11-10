-- Daily minutes in each heart rate zone: one row per participant per day.
-- The raw date is text, so it becomes a real date here.
-- coverage_share = minutes recorded / 1440. A day below 1.0 means the watch recorded
-- less than a full day (off wrist, charging, or a partial first/last day).
-- Zone names stay neutral (zone_1 to zone_3) until the Fitbit zone definitions are confirmed.

with source as (

    select * from {{ source('raw', 'raw_zone_minutes') }}

),

typed as (

    select
        participant_id,
        try_to_date(zone_date, 'YYYY-MM-DD') as zone_date,
        minutes_below_zone_1,
        minutes_zone_1,
        minutes_zone_2,
        minutes_zone_3,
        total_minutes,
        any_zone_missing,
        source_file,
        loaded_at
    from source

)

select
    participant_id || '|' || to_char(zone_date, 'YYYY-MM-DD') as zone_day_key,
    *,
    round(total_minutes / 1440, 4) as coverage_share
from typed
