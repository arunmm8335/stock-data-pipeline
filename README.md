# End-to-End Stock Data Pipeline

## Project Overview
This project is a full-stack Data Engineering pipeline that ingests, processes, and visualizes stock market data for the Nifty 50 index. It demonstrates the use of modern ETL tools to build a robust data lake.

## Tech Stack
* **Orchestration:** Apache Airflow (Scheduled Daily DAGs)
* **Processing:** Apache Spark / PySpark (Data Cleaning & Transformation)
* **Ingestion:** Python (yfinance API)
* **Storage:** Local Data Lake (Parquet format with Partitioning)
* **Visualization:** Streamlit + DuckDB

## Pipeline Architecture
1.  **Extract:** Pulls raw stock data from Yahoo Finance API.
2.  **Transform:** PySpark cleans data types and adds metadata.
3.  **Load:** Saves optimized Parquet files partitioned by date.
4.  **Visualize:** Streamlit dashboard reads directly from the Data Lake.

## Screenshots
*(Add screenshots of your Airflow DAG and Streamlit Dashboard here)*

## How to Run
1.  Install dependencies: `pip install -r requirements.txt`
2.  Start Airflow: `airflow standalone`
3.  Run Dashboard: `streamlit run dashboard.py`
