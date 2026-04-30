with facts as (
    select * from {{ ref('fact_articles') }}
),

daily as (
    select
        publish_date,
        category,
        count(*)                                as article_count,
        round(avg(sentiment_score)::numeric, 4) as avg_sentiment_score,
        sum(case when sentiment_label = 'POSITIVE' then 1 else 0 end) as positive_count,
        sum(case when sentiment_label = 'NEGATIVE' then 1 else 0 end) as negative_count,
        sum(case when sentiment_label = 'NEUTRAL'  then 1 else 0 end) as neutral_count
    from facts
    group by publish_date, category
)

select * from daily
order by publish_date desc, category
