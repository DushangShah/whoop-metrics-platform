-- Daily resting heart rate: one row per participant per day (14 of 16 participants have data).
-- A heart rate of 0 never comes with a date and is not a real reading, so it becomes NULL.
-- is_placeholder keeps that fact visible. error_raw is kept as-is: its meaning is not documented.

with source as (

    select * from {{ source('raw', 'raw_resting_heart_rate') }}

),

typed as (

    select
        participant_id,
        try_to_date(left(reading_datetime_raw, 10), 'YYYY-MM-DD') as reading_date,
        try_to_date(inner_date_raw, 'MM/DD/YY')                    as inner_date,
        resting_hr_raw,
        error_raw,
        source_file,
        loaded_at
    from source

)

select
    participant_id || '|' || to_char(reading_date, 'YYYY-MM-DD') as resting_hr_day_key,
    participant_id,
    reading_date,
    inner_date,
    (resting_hr_raw = 0)               as is_placeholder,
    nullif(resting_hr_raw, 0)          as resting_hr_bpm,
    error_raw,
    source_file,
    loaded_at
from typed
