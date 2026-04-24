import json, time, requests
from kafka import KafkaProducer
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

NEWSAPI_KEY   = os.getenv("NEWSAPI_KEY")
KAFKA_TOPIC   = "raw_news"
POLL_INTERVAL = 300
CATEGORIES    = ["technology","business","science","health","entertainment"]

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

def fetch_articles(category):
    r = requests.get("https://newsapi.org/v2/top-headlines", params={
        "category": category, "language": "en",
        "pageSize": 20, "apiKey": NEWSAPI_KEY,
    })
    r.raise_for_status()
    return r.json().get("articles", [])

def produce():
    print("Producer started. Polling every 5 minutes...")
    while True:
        for cat in CATEGORIES:
            try:
                articles = fetch_articles(cat)
                sent = 0
                for a in articles:
                    if not a.get("title") or not a.get("url"):
                        continue
                    record = {
                        "article_id":   str(hash(a["url"])),
                        "title":        a["title"],
                        "description":  a.get("description", ""),
                        "url":          a["url"],
                        "source":       a["source"]["name"],
                        "category":     cat,
                        "author":       a.get("author", "unknown"),
                        "published_at": a.get("publishedAt", ""),
                        "ingested_at":  datetime.utcnow().isoformat(),
                    }
                    producer.send(
                        KAFKA_TOPIC,
                        key=record["article_id"],
                        value=record
                    )
                    sent += 1
                print(f"[{cat}] Sent {sent} articles")
            except Exception as e:
                print(f"Error on {cat}: {e}")
        producer.flush()
        print(f"Sleeping {POLL_INTERVAL}s...")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    produce()