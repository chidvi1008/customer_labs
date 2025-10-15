import streamlit as st
import duckdb
import pandas as pd

con = duckdb.connect("data/customer_labs.duckdb")

st.title("📈 Real-time Attribution Dashboard")

st.subheader("Attribution Summary")
df = con.execute("select * from mart_attribution_agg").df()
st.dataframe(df)

st.subheader("Live Stream Events")
events = con.execute("select * from ga4_stream order by event_timestamp desc limit 20").df()
st.dataframe(events)
