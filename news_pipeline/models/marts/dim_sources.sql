with source as (
    select * from {{ ref('stg_articles') }}
),

sources as (
    select distinct
        md5(source)             as source_key,
        source                  as source_name,
        category,
        count(*) over (partition by source) as total_articles
    from source
)

select * from sources
