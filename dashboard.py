import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

# --- PAGE CONFIG ---
st.set_page_config(page_title="Stock Data Lake", page_icon="📈", layout="wide")

st.title("📈 Data Engineering Pipeline Results")
st.markdown("This dashboard reads directly from the **local Parquet Data Lake** built by Airflow & Spark.")

# --- DATA LOADING (Using DuckDB) ---
@st.cache_data
def load_data():
    # DuckDB can read ALL parquet files in nested folders using the glob pattern (*/*)
    query = """
    SELECT 
        trade_date, 
        ticker, 
        close_price, 
        volume, 
        processed_time 
    FROM 'datalake/processed/*/*.parquet'
    ORDER BY trade_date DESC
    """
    conn = duckdb.connect()
    df = conn.execute(query).df()
    conn.close()
    return df

try:
    df = load_data()
    
    # --- SIDEBAR FILTERS ---
    st.sidebar.header("Filters")
    
    # Get unique tickers from data
    all_tickers = df['ticker'].unique().tolist()
    selected_ticker = st.sidebar.selectbox("Select Ticker", all_tickers)

    # Filter the dataframe
    filtered_df = df[df['ticker'] == selected_ticker]

    # --- METRICS ROW ---
    # Get latest close price
    latest_price = filtered_df.iloc[0]['close_price']
    latest_date = filtered_df.iloc[0]['trade_date']
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Ticker", selected_ticker)
    col1.metric("Latest Price", f"₹{latest_price:,.2f}")
    col2.metric("Date", str(latest_date.date()))
    col3.metric("Records Found", len(filtered_df))

    # --- CHARTS ---
    st.subheader(f"Price History: {selected_ticker}")
    
    # Plotly Line Chart
    fig = px.line(
        filtered_df, 
        x='trade_date', 
        y='close_price', 
        markers=True,
        title=f"{selected_ticker} Closing Price"
    )
    st.plotly_chart(fig, use_container_width=True)

    # --- RAW DATA VIEW ---
    with st.expander("See Raw Data (Parquet Scan)"):
        st.dataframe(filtered_df)

except Exception as e:
    st.error(f"Error loading data: {e}")
    st.info("Hint: Did you run the Airflow pipeline to generate the Parquet files first?")
