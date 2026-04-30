with articles as (
    select * from {{ ref('stg_articles') }}
),

sources as (
    select * from {{ ref('dim_sources') }}
),

final as (
    select
        a.article_id,
        a.title,
        a.description,
        a.url,
        a.author,
        a.category,
        a.word_count,
        a.publish_date,
        a.published_at,
        a.processing_date,
        a.sentiment_label,
        a.sentiment_score,
        s.source_key,
        s.source_name
    from articles a
    left join sources s
        on a.source = s.source_name
)

select * from final
