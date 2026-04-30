with source as (
    select * from public.raw_gold_articles
),

cleaned as (
    select
        md5(coalesce(url, title || published_at::text)) as article_id,
        title,
        description,
        url,
        source,
        author,
        category,
        word_count,
        published_at,
        date(published_at)                as publish_date,
        processing_date,
        sentiment_label,
        sentiment_score::numeric(6,4)     as sentiment_score
    from source
    where title is not null
      and published_at is not null
)

select * from cleaned
