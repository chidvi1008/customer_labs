import streamlit as st
import duckdb
import pandas as pd

con = duckdb.connect("data/customer_labs.duckdb")

st.title("📈 Real-time Attribution Dashboard")

st.subheader("Attribution Summary")
df = con.execute("select * from mart_attribution_agg").df()
st.dataframe(df)
