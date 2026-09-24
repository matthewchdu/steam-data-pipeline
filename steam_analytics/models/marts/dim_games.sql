with source as (
    select * 
    from {{ source('bronze_dataset','games')}}
),

transformed as (
    select
        id as app_id,
        title,
        coalesce(developer,'Unknown Developer') as developer,
        round(cast(price as numeric), 2) as price,
        cast(release_date as date) as release_date,
        early_access as is_early_access,
        tags,
        genres,
        specs,
        (cast(price as numeric) = 0) as is_free_to_play,
        extract(year from cast(release_date as date)) as release_year
    from source
)

select * from transformed