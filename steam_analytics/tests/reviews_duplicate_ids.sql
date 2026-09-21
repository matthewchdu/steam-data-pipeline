select
    review_id,
    count(*) as frequency_count
from {{ ref('fct_reviews') }}
group by review_id
having count(*) > 1
