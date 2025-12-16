from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window
from dotenv import load_dotenv
import os
import pytz
from datetime import datetime

load_dotenv()
vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')
current_time = datetime.now(vietnam_tz)
day_crawl = current_time.date()

# Initialize Spark session with MongoDB connector
spark = SparkSession.builder \
    .appName("ProcessUnitsToMongoDB") \
    .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/tft_db.units") \
    .config("spark.jars", "/home/anhdq/.ivy2/jars/org.mongodb.spark_mongo-spark-connector_2.12-10.3.0.jar,/home/anhdq/.ivy2/jars/org.mongodb_mongodb-driver-sync-4.8.2.jar,/home/anhdq/.ivy2/jars/org.mongodb_bson-4.8.2.jar,/home/anhdq/.ivy2/jars/org.mongodb_mongodb-driver-core-4.8.2.jar") \
    .getOrCreate()

# Read Parquet files from HDFS
df = spark.read.parquet(f"hdfs://192.168.200.128:9000/tft/{day_crawl}/stream_output")

# Filter duplicate matches
df = df.withColumn("match_id", F.col("metadata.match_id")).dropDuplicates(["match_id"]).drop("match_id")
    
print("Total matches loaded from HDFS:", df.count())

# Filter standard game type and TFT Set 16
df = df.filter(F.col("info.tft_game_type") == "standard")
df = df.filter(F.col("info.tft_set_core_name") == "TFTSet16")

# Calculate total matches for frequency calculation
total_matches = df.count()
print(f"Total standard TFTSet16 matches: {total_matches}")

# Explode participants to get units
exploded_participants = df.select(
    F.col("metadata.match_id").alias("match_id"),
    F.posexplode("info.participants").alias("participant_index", "participant")
)

# Extract placement and units
units_df = exploded_participants.select(
    F.col("match_id"),
    F.col("participant.placement").alias("placement"),
    F.col("participant.units").alias("units")
)

# Explode units array to get individual units
flat_units = units_df.select(
    F.col("match_id"),
    F.col("placement"),
    F.explode("units").alias("unit")
).select(
    F.col("match_id"),
    F.col("placement"),
    F.col("unit.character_id").alias("unit_id"),
    F.col("unit.itemNames").alias("item_names"),
    F.col("unit.tier").alias("unit_tier")
)

# Clean unit_id by removing TFT16_ prefix, and item prefix
flat_units = flat_units.withColumn(
    "unit_id", 
    F.regexp_replace(F.regexp_replace(F.col("unit_id"), "TFT16_", ""), "tft16_", "")
)

# Apply function to clean prefix in item_names array
@F.udf(returnType=T.ArrayType(T.StringType()))
def clean_item_names_array(item_names):
    if item_names is None:
        return None
    cleaned_items = []
    for item in item_names:
        if item is not None:
            cleaned = item.replace("TFT_Item_", "").replace("TFT16_", "")
            cleaned_items.append(cleaned)
    return cleaned_items

flat_units = flat_units.withColumn(
    "item_names",
    clean_item_names_array(F.col("item_names"))
)

#print("Sample of flat units:")
#flat_units.show(10, truncate=False)

# Calculate popular items per unit
items_exploded = flat_units.select(
    "unit_id", 
    F.explode_outer("item_names").alias("item")
).filter(F.col("item").isNotNull())

# Rank items by frequency for each unit
items_ranked = items_exploded.groupBy("unit_id", "item").count().withColumn(
    "rn",
    F.row_number().over(Window.partitionBy("unit_id").orderBy(F.col("count").desc()))
)

# Get top 5 most popular items for each unit
top_items = (
    items_ranked.filter(F.col("rn") <= 5)
    .groupBy("unit_id")
    .agg(F.collect_list("item").alias("popular_items"))
)

#print("Popular items per unit:")
#top_items.show(40, truncate=False)

# Calculate unit statistics
unit_stats = (
    flat_units.groupBy("unit_id")
    .agg(
        F.avg("placement").alias("avg_place"),
        F.expr("avg(CASE WHEN placement = 1 THEN 1 ELSE 0 END) * 100").alias("win_rate"),
        F.count("*").alias("games_with_unit"),
    )
    .withColumn(
        "frequency",
        F.round(F.col("games_with_unit") / total_matches * 100, 2)
    )
)

# Define tier based on avg_place and win_rate
def tier_expr(avg_col, win_col):
    return (
        F.when((avg_col <= 4.1) & (win_col >= 12), F.lit("S"))
        .when((avg_col <= 4.4) & (win_col >= 8), F.lit("A"))
        .when((avg_col <= 4.8) & (win_col >= 5), F.lit("B"))
        .otherwise(F.lit("C"))
    )

unit_stats = unit_stats.withColumn("tier", tier_expr(F.col("avg_place"), F.col("win_rate")))

# Join with popular items
unit_stats = unit_stats.join(top_items, on="unit_id", how="left")

# Clean and format final dataframe
unit_stats_clean = (
    unit_stats.select(
        "unit_id",
        "tier",
        F.round("avg_place", 2).alias("avg_place"),
        F.round("win_rate", 2).alias("win_rate"),
        F.round("frequency", 2).alias("frequency"),
        "games_with_unit",
        "popular_items",
    )
    .orderBy("tier", "avg_place")
)

print(">>> Top 30 Units:")
unit_stats_clean.show(30, truncate=False)

# Write to MongoDB
unit_stats_clean.write.format("mongodb").mode("overwrite").option("database", "tft_db").option("collection", "units").save()

print(">>> Successfully written unit statistics to MongoDB (tft_db.units)")

spark.stop()
