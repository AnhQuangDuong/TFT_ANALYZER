from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from dotenv import load_dotenv
import os
import pytz
from datetime import datetime

load_dotenv()
vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')
current_time = datetime.now(vietnam_tz)
day_crawl = current_time.date()

# Initialize Spark Session with MongoDB connector
spark = SparkSession.builder \
    .appName("ProcessTraitsToMongoDB") \
    .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/tft_db.traits") \
    .config("spark.jars", "/home/anhdq/.ivy2/jars/org.mongodb.spark_mongo-spark-connector_2.12-10.3.0.jar,/home/anhdq/.ivy2/jars/org.mongodb_mongodb-driver-sync-4.8.2.jar,/home/anhdq/.ivy2/jars/org.mongodb_bson-4.8.2.jar,/home/anhdq/.ivy2/jars/org.mongodb_mongodb-driver-core-4.8.2.jar") \
    .getOrCreate()

# Read Parquet files from HDFS
df = spark.read.parquet(f"hdfs://192.168.200.128:9000/tft/{day_crawl}/stream_output")

# Filter duplicate matches
df = df.withColumn("match_id", F.col("metadata.match_id")).dropDuplicates(["match_id"]).drop("match_id")
    
print("Total matches loaded from HDFS:", df.count())

# Filter for standard games only
df = df.filter(F.col("info.tft_game_type") == "standard")

# Filter for TFTSet16 only
df = df.filter(F.col("info.tft_set_core_name") == "TFTSet16")

# Explode participants to get individual players
df_players = df.select(
    F.col("metadata.match_id").alias("match_id"),
    F.explode("info.participants").alias("player")
).select(
    "match_id",
    F.col("player.placement").alias("placement"),
    F.col("player.traits").alias("traits")
)

print("Total players:", df_players.count())

# Explode traits array for each player
df_traits = df_players.select(
    "match_id",
    "placement",
    F.explode("traits").alias("trait")
).select(
    "match_id",
    "placement",
    F.col("trait.name").alias("trait_id"),
    F.col("trait.num_units").alias("num_units"),
    F.col("trait.tier_current").alias("tier_current"),
    F.col("trait.tier_total").alias("tier_total"),
    F.col("trait.style").alias("style")
).filter(
    # Only get activated traits (tier_current > 0)
    F.col("tier_current") > 0
)

# Add win column (top 4 = win)
df_traits = df_traits.withColumn(
    "win", 
    (F.col("placement") <= 4).cast("int")
)

print(f"Total activated traits found: {df_traits.count():,}")

# Calculate trait statistics
total_traits = df_traits.count()

trait_stats = df_traits.groupBy("trait_id").agg(
    F.count("*").alias("count"),
    F.round(F.avg("placement"), 2).alias("avg_place"),
    F.round(F.avg("win") * 100, 1).alias("win_rate"),
    F.round(F.avg("tier_current"), 1).alias("avg_tier"),
    F.round(F.avg("num_units"), 1).alias("avg_units")
).withColumn(
    "frequency_pct", 
    F.round((F.col("count") / total_traits) * 100, 2)
)

# Analyze popular tier levels for each trait
trait_tier_stats = df_traits.groupBy("trait_id", "tier_current").agg(
    F.count("*").alias("tier_count"),
    F.round(F.avg("placement"), 2).alias("tier_avg_place"),
    F.round(F.avg("win") * 100, 1).alias("tier_win_rate")
)

# Get top 3 most popular tier levels for each trait
window_spec = Window.partitionBy("trait_id").orderBy(F.desc("tier_count"))

popular_tiers = trait_tier_stats.withColumn(
    "rank",
    F.row_number().over(window_spec)
).filter(
    F.col("rank") <= 3
).groupBy("trait_id").agg(
    F.collect_list(
        F.concat(F.lit("T"), F.col("tier_current").cast("string"))
    ).alias("popular_tiers")
)

# Join all statistics
final_result = trait_stats.join(
    popular_tiers, "trait_id", "left"
)

# Filter for popular traits (minimum 100 occurrences)
min_count = 100
popular_traits = final_result.filter(F.col("count") >= min_count)

# Strip TFT16_ prefix from trait_id
popular_traits = popular_traits.withColumn(
    "trait_id",
    F.regexp_replace(F.col("trait_id"), "TFT16_", "")
)

# Sort by avg_place (best performance first) and drop unnecessary columns
popular_traits_sorted = popular_traits.orderBy("avg_place").drop("place_change", "count", "tier")

print("\n=== TOP 20 TRAITS BY PERFORMANCE ===")
popular_traits_sorted.show(20, truncate=False)

# Write to MongoDB
popular_traits_sorted.write.format("mongodb") \
    .mode("overwrite") \
    .option("database", "tft_db") \
    .option("collection", "traits") \
    .save()

print(">>> Successfully wrote traits data to MongoDB (tft_db.traits)")

spark.stop()
