import os
import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# ── Page Config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="News Pipeline Analytics",
    page_icon="📰",
    layout="wide",
)

# ── Connection ────────────────────────────────────────────────────────────────
@st.cache_resource
def get_engine():
    return create_engine(os.getenv("NEON_CONNECTION_STRING"))

@st.cache_data(ttl=300)
def load_data():
    engine = get_engine()
    with engine.connect() as conn:
        facts = pd.DataFrame(
            conn.execute(text("SELECT * FROM analytics.fact_articles")).fetchall()
        ).set_axis(
            conn.execute(text("SELECT * FROM analytics.fact_articles LIMIT 0")).keys(),
            axis=1
        )
        trends = pd.DataFrame(
            conn.execute(text("SELECT * FROM analytics.agg_daily_trends")).fetchall()
        ).set_axis(
            conn.execute(text("SELECT * FROM analytics.agg_daily_trends LIMIT 0")).keys(),
            axis=1
        )
        sources = pd.DataFrame(
            conn.execute(text("SELECT * FROM analytics.dim_sources")).fetchall()
        ).set_axis(
            conn.execute(text("SELECT * FROM analytics.dim_sources LIMIT 0")).keys(),
            axis=1
        )
    facts["sentiment_score"] = pd.to_numeric(facts["sentiment_score"], errors="coerce")
    trends["avg_sentiment_score"] = pd.to_numeric(trends["avg_sentiment_score"], errors="coerce")
    sources["total_articles"] = pd.to_numeric(sources["total_articles"], errors="coerce")
    return facts, trends, sources

# ── Load Data ─────────────────────────────────────────────────────────────────
facts, trends, sources = load_data()

# ── Header ────────────────────────────────────────────────────────────────────
st.title("📰 Real-Time News Analytics Pipeline")
st.markdown("Live dashboard powered by NewsAPI → Kafka → Spark → dbt → Neon")
st.divider()

# ── KPI Row ───────────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Articles", len(facts))
col2.metric("Categories", facts["category"].nunique())
col3.metric("Sources", facts["source_name"].nunique())
col4.metric("Avg Sentiment", f"{pd.to_numeric(facts['sentiment_score'], errors='coerce').mean():.3f}")

st.divider()

# ── Row 1: Sentiment + Category ───────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Sentiment Distribution")
    sent_counts = facts["sentiment_label"].value_counts().reset_index()
    sent_counts.columns = ["sentiment", "count"]
    fig = px.pie(
        sent_counts, values="count", names="sentiment",
        color="sentiment",
        color_discrete_map={
            "POSITIVE": "#2ecc71",
            "NEGATIVE": "#e74c3c",
            "NEUTRAL":  "#3498db"
        }
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Articles by Category")
    cat_counts = facts["category"].value_counts().reset_index()
    cat_counts.columns = ["category", "count"]
    fig = px.bar(
        cat_counts, x="category", y="count",
        color="category",
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

# ── Row 2: Daily Trend + Top Sources ─────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Daily Sentiment Trend")
    fig = px.line(
        trends.sort_values("publish_date"),
        x="publish_date", y="avg_sentiment_score",
        color="category",
        markers=True,
        color_discrete_sequence=px.colors.qualitative.Set1
    )
    fig.update_layout(yaxis_title="Avg Sentiment Score")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Top News Sources")
    top_sources = (
        sources.sort_values("total_articles", ascending=False)
        .head(15)
    )
    fig = px.bar(
        top_sources, x="total_articles", y="source_name",
        orientation="h",
        color="total_articles",
        color_continuous_scale="Blues"
    )
    fig.update_layout(yaxis_title="", showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

# ── Row 3: Article Explorer ───────────────────────────────────────────────────
st.divider()
st.subheader("Article Explorer")

col1, col2 = st.columns(2)
with col1:
    category_filter = st.multiselect(
        "Filter by Category",
        options=facts["category"].unique(),
        default=facts["category"].unique()
    )
with col2:
    sentiment_filter = st.multiselect(
        "Filter by Sentiment",
        options=facts["sentiment_label"].unique(),
        default=facts["sentiment_label"].unique()
    )

filtered = facts[
    (facts["category"].isin(category_filter)) &
    (facts["sentiment_label"].isin(sentiment_filter))
]

st.dataframe(
    filtered[["title", "category", "sentiment_label", "sentiment_score", "publish_date", "source_name"]]
    .sort_values("publish_date", ascending=False)
    .reset_index(drop=True),
    use_container_width=True,
    height=400,
)

st.caption(f"Showing {len(filtered)} of {len(facts)} articles")
