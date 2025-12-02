from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from dotenv import load_dotenv
import os

load_dotenv()
#day_crawl = os.getenv("DAY_CRAWL")
day_crawl = "2025-11-25"

# Initialize Spark Session with MongoDB configuration
spark = SparkSession.builder \
    .appName("ProcessItemsToMongoDB") \
    .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/tft_db.items") \
    .config("spark.jars", "/home/anhdq/.ivy2/jars/org.mongodb.spark_mongo-spark-connector_2.12-10.3.0.jar,/home/anhdq/.ivy2/jars/org.mongodb_mongodb-driver-sync-4.8.2.jar,/home/anhdq/.ivy2/jars/org.mongodb_bson-4.8.2.jar,/home/anhdq/.ivy2/jars/org.mongodb_mongodb-driver-core-4.8.2.jar") \
    .getOrCreate()

# Read Parquet files from HDFS
df = spark.read.parquet(f"hdfs://192.168.200.128:9000/tft/{day_crawl}/stream_output")

# Remove duplicate matches
df = df.withColumn("match_id", F.col("data.metadata.match_id")).dropDuplicates(["match_id"]).drop("match_id")
    
print("Total matches loaded from HDFS:", df.count())

# Filter standard games and TFTSet15
df = df.filter(F.col("data.info.tft_game_type") == "standard")
df = df.filter(F.col("data.info.tft_set_core_name") == "TFTSet15")

# Step 1: Flatten data to get players
df_players = df.select(
    F.explode(F.col("data.info.participants")).alias("player")
).select(
    F.col("player.placement").alias("placement"),
    F.col("player.units").alias("units")
)

# Step 2: Flatten to get units
df_units = df_players.select(
    F.col("placement"),
    F.explode(F.col("units")).alias("unit")
).select(
    F.col("placement"),
    F.col("unit.character_id").alias("unit_name"),
    F.col("unit.itemNames").alias("itemNames")
)

# Step 3: Flatten to get items
df_items = df_units.select(
    F.col("placement"),
    F.col("unit_name"),
    F.explode(F.col("itemNames")).alias("item_id")
).filter(
    (F.col("item_id").isNotNull()) & 
    (F.col("item_id") != "") &
    (F.col("item_id") != "TFT_Item_EmptyBag")
)

# Add win column (top 4)
df_items = df_items.withColumn(
    "win", 
    (F.col("placement") <= 4).cast("int")
)

total_items = df_items.count()
print(f"Total items found: {total_items:,}")

# Step 4: Calculate item statistics
item_stats = df_items.groupBy("item_id").agg(
    F.count("*").alias("count"),
    F.round(F.avg("placement"), 2).alias("avg_place"),
    F.round(F.avg("win") * 100, 1).alias("win_rate")
).withColumn(
    "frequency_pct", 
    F.round((F.col("count") / total_items) * 100, 2)
)

# Step 5: Find Popular Units for each item
unit_item_counts = df_items.groupBy("item_id", "unit_name").count()
window_spec = Window.partitionBy("item_id").orderBy(F.desc("count"))

top_units_per_item = unit_item_counts.withColumn(
    "rank", 
    F.row_number().over(window_spec)
).filter(
    F.col("rank") <= 5
).groupBy("item_id").agg(
    F.collect_list("unit_name").alias("popular_units")
)

# Step 6: Calculate Baseline placement for all units
all_units_placements = df_units.select(
    F.col("placement"),
    F.col("unit_name")
).filter(F.col("unit_name").isNotNull())

unit_baseline = all_units_placements.groupBy("unit_name").agg(
    F.round(F.avg("placement"), 2).alias("baseline_place"),
    F.count("*").alias("total_appearances")
)

# Step 7: Calculate placement with item
unit_item_placements = df_items.groupBy("item_id", "unit_name").agg(
    F.round(F.avg("placement"), 2).alias("avg_place_with_item"),
    F.count("*").alias("count_with_item")
)

# Step 8: Calculate place change (impact)
unit_item_impact = unit_item_placements.join(
    unit_baseline,
    "unit_name",
    "inner"
).withColumn(
    "place_change",
    F.round(F.col("avg_place_with_item") - F.col("baseline_place"), 2)
)

# Step 9: Calculate average place change per item (weighted by usage count)
item_place_change = unit_item_impact.groupBy("item_id").agg(
    F.round(
        F.sum(F.col("place_change") * F.col("count_with_item")) / F.sum(F.col("count_with_item")), 
        2
    ).alias("avg_place_change")
)

# Step 10: Join all data
final_result = item_stats.join(top_units_per_item, "item_id", "left") \
                          .join(item_place_change, "item_id", "left") \
                          .orderBy("avg_place_change")

# Step 11: Filter popular items (minimum count threshold)
min_count = 100
popular_items = final_result.filter(F.col("count") >= min_count)

# Step 12: Clean item names
popular_items_cleaned = popular_items.withColumn(
    "item_name", 
    F.regexp_replace(
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(
                    F.regexp_replace(
                        F.regexp_replace(
                            F.regexp_replace(
                                F.col("item_id"), 
                                "TFT_Item_", ""
                            ), 
                            "TFT15_Item_", ""
                        ), 
                        "TFT4_Item_", ""
                    ), 
                    "_Artifact", ""
                ), 
                "Ornn", ""
            ), 
            "TFT5_Item_", ""
        ), 
        "Radiant|Artifact_", ""
    )
)

# Step 13: Clean popular units names (remove TFT15_ prefix)
@F.udf(returnType=T.ArrayType(T.StringType()))
def clean_unit_names(units):
    if units:
        return [unit.replace("TFT15_", "").replace("tft15_", "") for unit in units]
    return []

popular_items_final = popular_items_cleaned.withColumn(
    "popular_units_cleaned",
    clean_unit_names(F.col("popular_units"))
)

# Select final columns to save
final_output = popular_items_final.select(
    "item_name",
    "avg_place",
    "avg_place_change",
    "win_rate",
    "count",
    "frequency_pct",
    "popular_units_cleaned"
).orderBy("avg_place_change")

print("\n>>> Top 20 Items by Impact (Best First):")
final_output.show(20, truncate=False)

# Write to MongoDB
final_output.write.format("mongodb").mode("overwrite").option("database", "tft_db").option("collection", "items").save()

print("\n>>> Successfully wrote items data to MongoDB (tft_db.items)")

# Cleanup
spark.stop()
