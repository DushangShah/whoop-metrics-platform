-- Fails if more than 15% of rows are placeholders. About 8% today. The 15% limit is our choice.
select share_placeholder
from (
    select count_if(is_placeholder) / count(*) as share_placeholder
    from {{ ref('stg_pmdata__resting_heart_rate') }}
)
where share_placeholder > 0.15
