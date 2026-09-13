with source as (
    select * 
    from {{ source('bronze_dataset','reviews')}}
),

transformed as (
    select
        {{ dbt_utils.generate_surrogate_key(['username','product_id','date']) }} as review_id,
        {{ dbt_utils.generate_surrogate_key(['username']) }} as user_id,
        product_id as app_id,
        username,
        date as review_date,
        early_access as early_access_review,
        text as review_content,
        coalesce(array_length(regexp_extract_all(trim(text), r'\S+')), 0) as review_word_count,
        round(hours,2) as playtime_hours,
        
        case
            when hours < 2 then 'Within Refund Window'
            when hours between 2 and 20 then 'Standard (2-20 Hours)'
            else 'Veteran (20+ Hours)'
        end as playtime_tier,
    from source
    
)

select * from transformed