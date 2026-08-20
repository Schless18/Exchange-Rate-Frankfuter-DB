# Libraries
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Load Silver Layer

# 1. Time-Series Analysis
window_partition_time = Window.partitionBy("base_currency", "quote_currency").orderBy("rate_date")
# 7-Day
window_7d = Window.partitionBy("base_currency", "quote_currency").orderBy("rate_date").rowsBetween(-6, 0)
# 30-Day
window_30d = Window.partitionBy("base_currency", "quote_currency").orderBy("rate_date").rowsBetween(-29, 0)

# 2. Trends & Volatility
daily_insights = (
    silver_df
    # Prevous Day Rate
    .withColumn("previous_rate", F.lag("exchange_rate", 1).over(window_partition_time))
    .withColumn("daily_change", 
                F.when(F.col("previous_rate").isNotNull(), 
                       ((F.col("exchange_rate")-F.col("previous_rate")) / F.col("previous_rate")) * 100
                         ).otherwise(0))
    # Moving Averages
    .withColumn("sma_7d", F.avg("exchange_rate").over(window_7d))
    .withColumn("sma_30d", F.avg("exchange_rate").over(window_30d))
    # 30-Day Volatility
    .withColumn("volatility_30d", F.stddev("daily_change").over(window_30d))
    # Cast metrics to clean decimal representations
    .withColumn("daily_change", F.round("daily_change", 4))
    .withColumn("sma_7d", F.round("sma_7d", 6))
    .withColumn("sma_30d", F.round("sma_30d", 6))
    .withColumn("volatility_30d", F.round("volatility_30d", 4))
    .select("rate_date", "base_currency", "quote_currency", "exchange_rate", 
            "daily_change", "sma_7d", "sma_30d", "volatility_30d")
)

daily_insights.write \
.format("delta") \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.saveAsTable("frankfurter.default.gold_daily_trends")

print("Gold Layer Processing Completed.")

# Monthly Accounting Aggregates for Trend analysis
monthly_aggregates = (
    silver_df
    .withColumn("year_month", F.date_format("rate_date", "yyyy-MM"))        # Formating the date
    .groupBy("year_month", "base_currency", "quote_currency")               # Simple group by based on 3 fields
    .agg(                                                                   # Aggregating the data
        F.round(F.avg("exchange_rate"), 6).alias("monthly_avg_rate"),       # Average of the exchange rate
        F.round(F.min("exchange_rate"), 6).alias("monthly_min_rate"),       # Minimum of the exchange rate
        F.round(F.max("exchange_rate"), 6).alias("monthly_max_rate"),       # Maximum of the exchange rate
        F.count("rate_date").alias("trading_days_count")                    # Creates an alias of the count(total amount per rate date) for the rate date
    )
)

# Writting the data on the gold table, providing different attributes
monthly_aggregates.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("frankfurter.default.gold_monthly_aggregates")

# If successful prints the following message
print("Aggregation of the Gold Layer Completed.")

%sql
--SELECT * FROM frankfurter.default.gold_daily_trends;
--SELECT * FROM frankfurter.default.gold_monthly_aggregates;
