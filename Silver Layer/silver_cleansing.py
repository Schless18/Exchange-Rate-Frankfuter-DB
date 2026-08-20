#Libraries
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Load Bronze
bronze_df = spark.read.table("frankfurter.default.bronze_rates")

# 2. Data Cleansing
silver_df = (bronze_df 
    #Schema Enforecement
    .withColumn("rate_date", F.col("date").cast("date")) 
    .withColumn("base_currency", F.upper(F.trim(F.col("base")))) 
    .withColumn("quote_currency", F.upper(F.trim(F.col("quote")))) 
    # Cast to Decimal for better precision
    .withColumn("exchange_rate", F.col("rate").cast("decimal(18, 6)"))
    # Inverse rate
    .withColumn("inverse_rate", (F.lit(1) / F.col("exchange_rate")).cast("decimal(18, 6)"))

    # Business Logic & Quality Filters
    .filter(F.col("date").isNotNull())                                  # Filters out Null values from "date"
    .filter(F.col("base").isNotNull() & F.col("quote").isNotNull())     # Filters out Null values from "base"
    .filter(F.col("base_currency") != F.col("quote_currency"))          # Filters out EUR/EUR
    .filter(F.col("exchange_rate") > 0)                                 # Filters out negative exchange rates

    # Audit Column
    .withColumn("processed_at", F.current_timestamp())
    .select("rate_date", "base_currency", "quote_currency", "exchange_rate", "inverse_rate", "processed_at")
)

# 3. Duplicates (Keep latest record per date + pair)
duplicates = Window.partitionBy("rate_date", "base_currency", "quote_currency").orderBy(F.col("processed_at").desc())

silver_duprem_df = (
    silver_df
    .withColumn("row_num", F.row_number().over(duplicates))         # Adds column "row_num" to see how many duplicates are there
    .filter(F.col("row_num") == 1)                                  # Keeps only the latest entry, if there are multiple duplicates 
    .drop("row_num")                                                # Drops the extra column used for the previous filter
)

# 4. Write to Silver Delta Table
silver_duprem_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("frankfurter.default.silver_rates")

print("Silver Layer created succesfully")

%sql 
SELECT * FROM frankfurter.default.silver_rates
