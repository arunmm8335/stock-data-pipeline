from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from datetime import datetime
import os
import sys

# CONFIGURATION
TODAY = datetime.now().strftime("%Y-%m-%d")
INPUT_PATH = os.path.join("datalake", "raw", TODAY)
OUTPUT_PATH = os.path.join("datalake", "processed", TODAY)

print(f"--- ⚙️ Starting Spark Processing for {TODAY} ---")

# 1. Initialize Spark Session
try:
    spark = SparkSession.builder \
        .appName("StockCleaner") \
        .master("local[*]") \
        .getOrCreate()
    # Suppress heavy logging
    spark.sparkContext.setLogLevel("ERROR")
except Exception as e:
    print(f"❌ Error starting Spark: {e}")
    sys.exit(1)

# 2. Check if raw data exists
if not os.path.exists(INPUT_PATH):
    print(f"❌ Error: No data found at {INPUT_PATH}. Run ingest.py first.")
    spark.stop()
    sys.exit(1)

# 3. Read Raw CSV Data
print(f"Reading data from {INPUT_PATH}...")
try:
    raw_df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True)
except Exception as e:
    print(f"❌ Error reading CSVs: {e}")
    spark.stop()
    sys.exit(1)

# 4. Transform / Clean
# We cast columns to correct types and rename them
cleaned_df = raw_df.select(
    col("Date").alias("trade_date"),
    col("Open").cast("double").alias("open_price"),
    col("High").cast("double").alias("high_price"),
    col("Low").cast("double").alias("low_price"),
    col("Close").cast("double").alias("close_price"),
    col("Volume").cast("long").alias("volume"),
    col("ticker")
).withColumn("processed_time", current_timestamp())

# 5. Write to Parquet (The "Load" step)
print(f"Writing clean data to {OUTPUT_PATH}...")
cleaned_df.write.mode("overwrite").parquet(OUTPUT_PATH)

print("✅ Success! Pipeline Finished.")
spark.stop()
