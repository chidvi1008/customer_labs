import streamlit as st
import duckdb
import pandas as pd

con = duckdb.connect("data/customer_labs.duckdb")

st.title("📈 Real-Time Attribution Dashboard")

st.subheader("🧭 Attribution Summary")
df = con.execute("SELECT * FROM mart_attribution_agg").df()
st.dataframe(df, use_container_width=True)

st.subheader("📊 Session Data")
df = con.execute("SELECT * FROM int_events").df()
st.dataframe(df, use_container_width=True)

st.subheader("Live panel events")
df = con.execute("SELECT * FROM stg_events").df()
st.dataframe(df, use_container_width=True)
