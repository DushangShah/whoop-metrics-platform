-- Cleaned wellness reports: one row per participant per self-reported entry.
-- Ratings are 1 to 5 (readiness is 0 to 10).
-- Decision (checked 2026-11-05): a 0 on a 1 to 5 rating is treated as "not answered" and set to NULL.
-- If all five 1 to 5 ratings are 0, the entry is flagged is_blank_entry and
-- readiness and sleep_duration_hours are also set to NULL.

with source as (

    select * from {{ source('raw', 'raw_wellness') }}

),

typed as (

    select
        participant_id,
        coalesce(
            try_to_timestamp_ntz(effective_time_frame, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'),
            try_to_timestamp_ntz(effective_time_frame, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
        )                                          as reported_at_utc,
        try_to_double(fatigue)                     as fatigue_raw,
        try_to_double(mood)                        as mood_raw,
        try_to_double(readiness)                   as readiness_raw,
        try_to_double(sleep_duration_h)            as sleep_duration_raw,
        try_to_double(sleep_quality)               as sleep_quality_raw,
        try_to_double(soreness)                    as soreness_raw,
        try_to_double(stress)                      as stress_raw,
        soreness_area                              as soreness_area_raw,
        effective_time_frame                       as reported_at_raw,
        source_file,
        loaded_at
    from source

),

flagged as (

    select
        *,
        coalesce(
            fatigue_raw = 0 and mood_raw = 0 and sleep_quality_raw = 0
            and soreness_raw = 0 and stress_raw = 0,
            false
        ) as is_blank_entry
    from typed

),

cleaned as (

    select
        participant_id,
        reported_at_utc,
        reported_at_raw,
        is_blank_entry,
        nullif(fatigue_raw, 0)                     as fatigue,
        nullif(mood_raw, 0)                        as mood,
        iff(is_blank_entry, null, readiness_raw)   as readiness,
        iff(is_blank_entry, null, sleep_duration_raw) as sleep_duration_hours,
        nullif(sleep_quality_raw, 0)               as sleep_quality,
        nullif(soreness_raw, 0)                    as soreness,
        nullif(stress_raw, 0)                      as stress,
        soreness_area_raw,
        source_file,
        loaded_at
    from flagged

)

select
    participant_id || '|' || reported_at_raw as wellness_key,
    *
from cleaned
