### Architecture
**Tools:** dbt + DuckDB + Streamlit + Python

**Flow:**
GA4 Events (CSV/stream) → dbt (stg → int → mart) → DuckDB → Streamlit dashboard.

**Key Design Choices**
- Local DuckDB replaces BigQuery for demo
- First/Last-click attribution at user level
- Streaming simulated via `stream_demo.py`
- Dashboard auto-reads latest DuckDB tables
