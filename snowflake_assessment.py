import streamlit as st
import snowflake.connector
import pandas as pd

# ---------- Basic setup ----------
st.set_page_config(page_title="Superstore — Gold", layout="wide")
st.title("📊 Superstore — Assessment")

# ---------- Snowflake Connection ----------
@st.cache_resource(show_spinner=False)
def get_connection():
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"],
    )

conn = get_connection()

# ---------- Helper for dataframes ----------
@st.cache_data(show_spinner=False, ttl=300)
def run_query_df(sql: str) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql)
        try:
            df = cur.fetch_pandas_all()
        except Exception:
            df = pd.DataFrame()
    return df

def show_df_and_chart(df: pd.DataFrame, index_col: str, value_col, chart="bar"):
    if df.empty:
        st.info("Sin datos para mostrar.")
        return
    # Force numeric types
    if isinstance(value_col, list):
        for c in value_col:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
    else:
        if value_col in df.columns:
            df[value_col] = pd.to_numeric(df[value_col], errors="coerce")

    # Graphic
    if index_col in df.columns:
        plot_df = df.set_index(index_col)
        if isinstance(value_col, list):
            plot_df = plot_df[value_col]
        else:
            plot_df = plot_df[[value_col]]
        if chart == "bar":
            st.bar_chart(plot_df)
        elif chart == "line":
            st.line_chart(plot_df)

    # Table
    st.dataframe(df, use_container_width=True)

# ---------- Assesment Tabs ----------
t1, t2, t3, t4, t5, t6, t7 = st.tabs([
    "1) Sales/Orders/Profit by Year",
    "2) Orders by DOW (2017)",
    "3) Sales by Segment",
    "4) Top 10 Products (2017)",
    "5) Bottom 10 Products (2015)",
    "6) Returns (2015)",
    "7) Returns by Manager (2015)"
])

# ---------- Content ----------
with t1:
    st.subheader("1) Orders, Sales, Profit by Year")
    d = run_query_df("SELECT * FROM GOLD.SALES_BY_YEAR ORDER BY YEAR")
    kc1, kc2, kc3 = st.columns(3)
    if not d.empty:
        kc1.metric("Years", f"{len(d)}")
        kc2.metric("Total Sales", f"{d['SALES'].sum():,.2f}")
        kc3.metric("Total Profit", f"{d['PROFIT'].sum():,.2f}")
    show_df_and_chart(d, index_col="YEAR", value_col=["SALES", "PROFIT"], chart="bar")

with t2:
    st.subheader("2) Orders by Day of Week (2017)")
    d = run_query_df("SELECT * FROM GOLD.ORDERS_BY_DOW_2017")
    show_df_and_chart(d, index_col="DOW_ABBR", value_col="ORDERS", chart="bar")

with t3:
    st.subheader("3) Sales by Segment (All Years)")
    d = run_query_df("SELECT * FROM GOLD.SALES_BY_SEGMENT")
    show_df_and_chart(d, index_col="SEGMENT", value_col="SALES", chart="bar")

with t4:
    st.subheader("4) Top 10 Products by Sales (2017)")
    d = run_query_df("SELECT * FROM GOLD.TOP10_PRODUCTS_2017")
    show_df_and_chart(d, index_col="PRODUCT_NAME", value_col="SALES", chart="bar")

with t5:
    st.subheader("5) Bottom 10 Products by Sales (2015)")
    d = run_query_df("SELECT * FROM GOLD.BOTTOM10_PRODUCTS_2015")
    show_df_and_chart(d, index_col="PRODUCT_NAME", value_col="SALES", chart="bar")

with t6:
    st.subheader("6) Returned Orders in 2015 (count & sales)")
    d = run_query_df("SELECT * FROM GOLD.RETURNS_2015")
    if not d.empty:
        c1, c2 = st.columns(2)
        c1.metric("Returned Orders (2015)", f"{int(pd.to_numeric(d['RETURNED_ORDERS']).iloc[0])}")
        c2.metric("Returned Sales (2015)", f"{float(pd.to_numeric(d['RETURNED_SALES']).iloc[0]):,.2f}")
    st.dataframe(d, use_container_width=True)

with t7:
    st.subheader("7) Returned Orders by Manager (2015)")
    d = run_query_df("SELECT * FROM GOLD.RETURNS_BY_MANAGER_2015 ORDER BY RETURNED_SALES DESC")
    show_df_and_chart(d, index_col="REGIONAL_MANAGER", value_col="RETURNED_SALES", chart="bar")

st.caption("Data sources: SILVER.ORDERS_ENRICHED and GOLD views generated from RAW loads.")
