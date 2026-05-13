import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

LOCAL_URL = (
    f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)
NEON_URL = os.getenv("NEON_CONNECTION_STRING")

local_engine = create_engine(LOCAL_URL)
neon_engine  = create_engine(NEON_URL)

TABLES = [
    ("public",    "raw_gold_articles"),
    ("analytics", "fact_articles"),
    ("analytics", "dim_sources"),
    ("analytics", "agg_daily_trends"),
]

def sync_table(schema, table):
    full_name = f"{schema}.{table}"
    print(f"Syncing {full_name}...")

    with local_engine.connect() as local_conn:
        result = local_conn.execute(text(f'SELECT * FROM "{schema}"."{table}"'))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    print(f"  Read {len(df)} rows from local")

    # Pass engine directly — pandas handles the connection internally
    df.to_sql(
        table,
        neon_engine,
        schema=schema,
        if_exists="replace",
        index=False,
    )
    print(f"  ✓ Written to Neon: {full_name}")

def main():
    with neon_engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics"))
        print("✓ Analytics schema ready in Neon")

    for schema, table in TABLES:
        sync_table(schema, table)

    print("\n✅ All tables synced to Neon successfully!")

if __name__ == "__main__":
    main()