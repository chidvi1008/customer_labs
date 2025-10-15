import duckdb
import pandas as pd
from datetime import datetime
import uuid
import random

con = duckdb.connect("my_db.duckdb")

# Truncate stage table
con.execute("DELETE FROM stg_events")

# Simulate fetching events from GA4 API
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

# Insert into staging
con.execute("INSERT INTO stg_events SELECT * FROM df")
