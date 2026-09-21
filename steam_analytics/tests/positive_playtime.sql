select 
    review_id,
    playtime_hours
from  {{ ref('fct_reviews')}}
where
    playtime_hours < 0