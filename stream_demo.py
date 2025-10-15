import time, random, duckdb, pandas as pd
from datetime import datetime

events = [
    {"event_name": "page_view"},
    {"event_name": "add_to_cart"},
    {"event_name": "purchase"}
]

con = duckdb.connect("data/customer_labs.duckdb")
con.execute("CREATE TABLE IF NOT EXISTS ga4_stream AS SELECT * FROM read_csv_auto('data/sample_ga4_events.csv') LIMIT 0;")

for i in range(10):
    e = random.choice(events)
    row = {
        "event_timestamp": datetime.utcnow().isoformat(),
        "event_name": e["event_name"],
        "session_id": f"s{i%3}",
        "user_pseudo_id": f"user{i%2}",
        "source": random.choice(["google", "facebook", "email"]),
        "medium": random.choice(["cpc", "organic", "referral"]),
        "campaign": random.choice(["summer_sale", "new_launch"]),
        "page_location": "/home"
    }
    df = pd.DataFrame([row])
    con.execute("INSERT INTO ga4_stream SELECT * FROM df")
    print(f"Streamed: {row}")
    time.sleep(2)
