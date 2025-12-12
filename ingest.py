import yfinance as yf
import pandas as pd
import os
from datetime import datetime

# CONFIGURATION
TICKERS = ["RELIANCE.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS"]
TODAY = datetime.now().strftime("%Y-%m-%d")
# We create a specific folder for today's data
BASE_PATH = os.path.join("datalake", "raw", TODAY)

# Ensure the directory exists
os.makedirs(BASE_PATH, exist_ok=True)

print(f"--- 📥 Starting Ingestion for {TODAY} ---")

for ticker in TICKERS:
    try:
        print(f"Fetching {ticker}...")
        stock = yf.Ticker(ticker)
        
        # Fetch 1 day of data
        df = stock.history(period="1d")
        
        if not df.empty:
            # Add metadata
            df['ticker'] = ticker
            
            # Save to CSV
            file_name = f"{ticker}.csv"
            file_path = os.path.join(BASE_PATH, file_name)
            df.to_csv(file_path)
            print(f"✅ Saved: {file_path}")
        else:
            print(f"⚠️ No data found for {ticker}")

    except Exception as e:
        print(f"❌ Error fetching {ticker}: {e}")
