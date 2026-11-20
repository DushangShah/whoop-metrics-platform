-- One row per sleep record per participant. Repeated copies of a logId are collapsed to one row.
-- Dropped on purpose: the 30-day-average columns and the summary text. They differ between repeated
-- copies and are not needed for nightly metrics (they stay in the raw table).
-- Stage columns for "stages" records (deep, light, rem, wake) and "classic" records (asleep, restless,
-- awake) stay separate because they are different measurements.
-- start/end times are local clock readings with no time zone. Do not join them to UTC timestamps.
-- efficiency_pct is NULL where the raw efficiency is 0 (see docs/dataset.md).

with source as (

    select * from {{ source('raw', 'raw_sleep_logs') }}

),

deduped as (

    select *
    from source
    qualify row_number() over (
        partition by participant_id, log_id
        order by levels_summary_json, loaded_at
    ) = 1

),

typed as (

    select
        participant_id,
        log_id,
        try_to_date(date_of_sleep, 'YYYY-MM-DD')                          as sleep_date,
        try_to_timestamp_ntz(start_time_raw, 'YYYY-MM-DD HH24:MI:SS')     as start_time_local,
        try_to_timestamp_ntz(end_time_raw, 'YYYY-MM-DD"T"HH24:MI:SS.FF3') as end_time_local,
        sleep_type,
        main_sleep                                                         as is_main_sleep,
        time_in_bed                                                        as time_in_bed_minutes,
        minutes_asleep,
        minutes_awake,
        minutes_to_fall_asleep,
        minutes_after_wakeup,
        time_in_bed - (minutes_asleep + minutes_awake)                     as time_in_bed_gap_minutes,
        efficiency                                                         as efficiency_raw,
        nullif(efficiency, 0)                                              as efficiency_pct,
        info_code,
        deep_count, deep_minutes,
        light_count, light_minutes,
        rem_count, rem_minutes,
        wake_count, wake_minutes,
        asleep_count     as classic_asleep_count,
        asleep_minutes   as classic_asleep_minutes,
        restless_count   as classic_restless_count,
        restless_minutes as classic_restless_minutes,
        awake_count      as classic_awake_count,
        awake_minutes    as classic_awake_minutes,
        source_file,
        loaded_at
    from deduped

)

select
    participant_id || '|' || log_id as sleep_log_key,
    *
from typed
