from pyspark.sql import SparkSession
import sys

# Initialize Spark
spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read the Parquet folder
try:
    df = spark.read.parquet("datalake/processed/2025-12-12")
    
    print("--- 📊 Data Schema ---")
    df.printSchema()
    
    print("--- 🧐 Top 5 Rows ---")
    df.show(5)
    
    print(f"Total Row Count: {df.count()}")

except Exception as e:
    print(e)
