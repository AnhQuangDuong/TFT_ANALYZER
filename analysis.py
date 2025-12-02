import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Load Mongo URI from env (default to Atlas URI provided)
mongo_base_uri = os.getenv(
    "MONGO_URI",
    "mongodb://localhost:27017",
)
mongo_write_uri = f"{mongo_base_uri.rstrip('/')}/tft_db.unit_stats"

# Spark session
spark = (
    SparkSession.builder.appName("TFT-Unit-Analysis")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0")
    .config("spark.mongodb.write.connection.uri", mongo_write_uri)
    # Bypass local CRC issues when reading JSON on Windows
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    .getOrCreate()
)

# Đọc dữ liệu tương tự unit_tier_list (recursive + infer schema)
print(">>> Doc du lieu JSON trong ./data_3580_matches ...")
df = (
    spark.read.option("recursiveFileLookup", "true")
    .option("inferSchema", "true")
    .json("./data_3580_matches")
)
if "data" in df.columns and "info" not in df.columns:
    df = df.select("data.*")
df.cache()

if "metadata" in df.columns and "match_id" in df.select("metadata.*").columns:
    total_matches = df.select(F.col("metadata.match_id")).distinct().count()
else:
    total_matches = df.count()
print(f">>> Tong so tran: {total_matches:,}")

# Explode units with placement
units_df = (
    df.select(
        F.col("metadata.match_id").alias("match_id"),
        F.col("info.participants").alias("participants"),
    )
    .select(F.explode("participants").alias("p"))
    .select(
        F.col("p.units").alias("units"),
        F.col("p.placement").alias("placement"),
    )
    .select(F.explode("units").alias("u"), "placement")
)

flat_units = units_df.select(
    F.col("u.character_id").alias("unit_id"),
    F.col("u.itemNames").alias("item_names"),
    F.col("placement"),
)

# Popular items per unit (top 5)
items_exploded = flat_units.select("unit_id", F.explode_outer("item_names").alias("item"))

items_ranked = items_exploded.groupBy("unit_id", "item").count().withColumn(
    "rn",
    F.row_number().over(Window.partitionBy("unit_id").orderBy(F.col("count").desc())),
)

top_items = (
    items_ranked.filter(F.col("rn") <= 5)
    .groupBy("unit_id")
    .agg(F.collect_list("item").alias("popular_items"))
)

unit_stats = (
    flat_units.groupBy("unit_id")
    .agg(
        F.avg("placement").alias("avg_place"),
        F.expr("avg(CASE WHEN placement = 1 THEN 1 ELSE 0 END) * 100").alias("win_rate"),
        F.count("*").alias("games_with_unit"),
    )
    .withColumn(
        "frequency",
        F.when(F.lit(total_matches) > 0, F.col("games_with_unit") / total_matches * 100).otherwise(0),
    )
)


def tier_expr(avg_col, win_col):
    return (
        F.when((avg_col <= 4.0) & (win_col >= 15), F.lit("S"))
        .when((avg_col <= 4.5) & (win_col >= 10), F.lit("A"))
        .when((avg_col <= 5.0) & (win_col >= 5), F.lit("B"))
        .otherwise(F.lit("C"))
    )


unit_stats = unit_stats.withColumn("tier", tier_expr(F.col("avg_place"), F.col("win_rate")))
unit_stats = unit_stats.join(top_items, on="unit_id", how="left")

unit_stats_clean = (
    unit_stats.select(
        "unit_id",
        "tier",
        F.round("avg_place", 2).alias("avg_place"),
        F.round("win_rate", 2).alias("win_rate"),
        F.round("frequency", 2).alias("frequency"),
        "popular_items",
    )
    .orderBy(F.col("tier"), F.col("avg_place"))
)

print(">>> Top 20 unit stats:")
unit_stats_clean.show(20, truncate=False)

unit_stats_clean.write.format("mongodb").mode("overwrite").option("database", "tft_db").option(
    "collection", "unit_stats"
).save()

spark.stop()
