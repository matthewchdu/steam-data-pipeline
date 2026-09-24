with source as (
    select * 
    from {{ source('bronze_dataset','reviews')}}
),

deduplicated_users as (
    select
        username,
        max(products) as games_owned
    from source
    where username is not null
    group by username
),

transformed as (
    select
        {{ dbt_utils.generate_surrogate_key(['username']) }} as user_id,
        username,
        games_owned,
        case
            when games_owned is null or games_owned = 0 then '0 Games / Private Profile'
            when games_owned < 10 then 'Casual (<10)'
            when games_owned between 10 and 100 then 'Core Gamer (10-100)'
            else 'Collector (100+)'
        end as library_size_tier
    from deduplicated_users
)

select * from transformed