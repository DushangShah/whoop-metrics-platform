-- A participant should have at most one main sleep per dateOfSleep.
select participant_id, sleep_date, count(*) as main_sleeps
from {{ ref('stg_pmdata__sleep_logs') }}
where is_main_sleep
group by participant_id, sleep_date
having count(*) > 1
