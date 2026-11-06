-- Cleaned sleep scores: one row per participant per sleep log.
-- Raw text becomes real numbers and timestamps. Anything that fails to convert becomes NULL,
-- and the tests in _staging.yml then flag it.

with source as (

    select * from {{ source('raw', 'raw_sleep_score') }}

),

renamed as (

    select
        participant_id,
        try_to_number(sleep_log_entry_id)                                   as sleep_log_id,
        try_to_timestamp_ntz(timestamp_raw, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')   as score_timestamp_utc,
        try_to_double(overall_score)                                        as overall_score,
        try_to_double(composition_score)                                    as composition_score,
        try_to_double(revitalization_score)                                 as revitalization_score,
        try_to_double(duration_score)                                       as duration_score,
        try_to_double(deep_sleep_in_minutes)                                as deep_sleep_minutes,
        try_to_double(resting_heart_rate)                                   as resting_heart_rate_bpm,
        try_to_double(restlessness)                                         as restlessness,
        source_file,
        loaded_at
    from source

)

select
    participant_id || '-' || sleep_log_id::varchar as sleep_score_key,
    *
from renamed
