import duckdb
import pandas as pd
from datetime import datetime
import uuid
import random

# Connect to DuckDB database
con = duckdb.connect("data/customer_labs.duckdb")

# Create table schema (runs only if it doesn’t exist)
con.execute("""
CREATE TABLE IF NOT EXISTS stg_events (
    event_id TEXT,
    user_pseudo_id TEXT,
    event_name TEXT,
    source TEXT,
    medium TEXT,
    campaign TEXT,
    event_timestamp TIMESTAMP,
    session_id TEXT,
    page_location TEXT
);
""")

# Simulate fetching events
events = []
for i in range(20):
    events.append({
        "event_id": str(uuid.uuid4()),
        "user_pseudo_id": f"user_{random.randint(1,5)}",
        "event_name": random.choice(["page_view", "click", "purchase"]),
        "source": random.choice(["google", "facebook", "newsletter"]),
        "medium": random.choice(["cpc", "organic", "email"]),
        "campaign": random.choice(["campaign_1", "campaign_2"]),
        "event_timestamp": datetime.now(),
        "session_id": f"session_{random.randint(1,5)}",
        "page_location": f"/page_{random.randint(1,5)}"
    })

df = pd.DataFrame(events)

# checking if table exists or not
con.register("df_view", df)

# Insert data into the table
con.execute("INSERT INTO stg_events SELECT * FROM df_view")

print("20 events inserted successfully!")
