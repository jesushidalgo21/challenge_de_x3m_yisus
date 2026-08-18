import os

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="X3M - Product Daily Revenue", layout="wide")


@st.cache_resource
def get_engine():
    url = (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    return create_engine(url)


@st.cache_data(ttl=30)
def load_data():
    try:
        with get_engine().connect() as conn:
            return pd.read_sql(
                text(
                    "SELECT product_id, product_title, date, units_sold, revenue "
                    "FROM product_daily_revenue"
                ),
                conn,
            )
    except Exception:
        return None


st.title("Product Daily Revenue")

df = load_data()

if df is None or df.empty:
    st.info(
        "No data yet. Run the pipeline "
        "(`docker compose exec airflow-scheduler airflow dags backfill ...`) "
        "and reload this page."
    )
    st.stop()

df["date"] = pd.to_datetime(df["date"])

st.subheader("Total revenue per day")
daily_revenue = df.groupby("date", as_index=False)["revenue"].sum().sort_values("date")
st.line_chart(daily_revenue.set_index("date")["revenue"])

st.subheader("Top products by cumulative revenue")
top_n = st.slider("Number of products", min_value=5, max_value=30, value=10)
top_products = (
    df.groupby("product_title", as_index=False)["revenue"]
    .sum()
    .sort_values("revenue", ascending=False)
    .head(top_n)
)
st.bar_chart(top_products.set_index("product_title")["revenue"])

st.subheader("Detail")
col1, col2 = st.columns(2)
with col1:
    date_options = sorted(df["date"].dt.date.unique(), reverse=True)
    selected_dates = st.multiselect("Date", options=date_options)
with col2:
    product_options = sorted(df["product_title"].dropna().unique())
    selected_products = st.multiselect("Product", options=product_options)

filtered = df.copy()
if selected_dates:
    filtered = filtered[filtered["date"].dt.date.isin(selected_dates)]
if selected_products:
    filtered = filtered[filtered["product_title"].isin(selected_products)]

st.dataframe(
    filtered.sort_values(["date", "revenue"], ascending=[False, False]),
    use_container_width=True,
)
