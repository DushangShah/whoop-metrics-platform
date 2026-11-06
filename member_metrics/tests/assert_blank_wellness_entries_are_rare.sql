-- Fails if more than 1% of wellness entries are blank (all ratings 0).
select share_blank
from (
    select count_if(is_blank_entry) / count(*) as share_blank
    from {{ ref('stg_pmdata__wellness') }}
)
where share_blank > 0.01
