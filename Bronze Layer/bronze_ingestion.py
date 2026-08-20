# Importing Libraries
import requests
import json
from datetime import datetime
from pyspark.sql.functions import current_timestamp, lit, explode, map_keys
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Fetching Frankfurter API
top_currencies = "USD,EUR,GBP,JPY,CAD,AUD,CHF,CNY,HKD,NZD,MXN"
API_URL = f"https://api.frankfurter.dev/v2/rates?from=2024-01-01&symbols={top_currencies}"
response = requests.get(API_URL, timeout=10)

#Bronze data frame
if response.status_code == 200:
    raw_data = response.json()                      # raw_data is already a list of dicts
    schema = StructType([
        StructField("date", StringType(), True),
        StructField("base", StringType(), True),
        StructField("quote", StringType(), True),
        StructField("rate", DoubleType(), True)
    ])
    bronze_df = spark.createDataFrame(raw_data, schema) 
    #Write to bronze table
    bronze_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("frankfurter.default.bronze_rates")
    print("Bronze layer was successful")
else:
    print("Failed to write the rquested information"

%sql
SELECT * FROM frankfurter.default.bronze_rates
