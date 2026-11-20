-- Fails if more than 1% of sleep records have a raw efficiency of 0. It is 2 of 2,001 today.
select share_zero
from (
    select count_if(efficiency_raw = 0) / count(*) as share_zero
    from {{ ref('stg_pmdata__sleep_logs') }}
)
where share_zero > 0.01
