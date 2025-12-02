from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
import pandas as pd
import matplotlib.pyplot as plt

# Spark session
spark = (
    SparkSession.builder
    .appName("TFT-Unit-Tier-List")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

print(">>> Đang đọc dữ liệu JSON...")
df = (
    spark.read.option("recursiveFileLookup", "true")
    .option("inferSchema", "true")
    .json("./data_3580_matches")
)
if "data" in df.columns and "info" not in df.columns:
    df = df.select("data.*")
df.cache()
print(f">>> Loaded {df.count():,} trận.")

if "metadata" in df.columns and "match_id" in df.select("metadata.*").columns:
    total_matches = df.select(F.col("metadata.match_id")).distinct().count()
else:
    total_matches = df.count()
print(f">>> Total matches: {total_matches:,}")

# Explode units
units_df = (
    df.select(F.explode(F.col("info.participants")).alias("p"))
    .select(
        F.col("p.placement").alias("placement"),
        F.col("p.units").alias("units"),
    )
    .select(F.explode("units").alias("u"), "placement")
)

flat_units = units_df.select(
    F.col("u.character_id").alias("unit_id"),
    F.col("u.itemNames").alias("item_names"),
    F.col("placement"),
)

# Popular items per unit
items_exploded = flat_units.select(
    "unit_id", F.explode_outer("item_names").alias("item")
).filter(F.col("item").isNotNull())

items_ranked = items_exploded.groupBy("unit_id", "item").count().withColumn(
    "rn",
    F.row_number().over(Window.partitionBy("unit_id").orderBy(F.col("count").desc())),
)

top_items = (
    items_ranked.filter(F.col("rn") <= 5)
    .groupBy("unit_id")
    .agg(F.collect_list("item").alias("popular_items"))
)

# Unit stats
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
        F.when((avg_col <= 4.1) & (win_col >= 12), F.lit("S"))
        .when((avg_col <= 4.4) & (win_col >= 8), F.lit("A"))
        .when((avg_col <= 4.8) & (win_col >= 5), F.lit("B"))
        .otherwise(F.lit("C"))
    )

unit_stats = unit_stats.withColumn("tier", tier_expr(F.col("avg_place"), F.col("win_rate")))
unit_stats = unit_stats.join(top_items, on="unit_id", how="left")

unit_stats_clean = (
    unit_stats.select(
        "unit_id",
        "tier",
        F.round("avg_place", 2).alias("Avg Place"),
        F.round("win_rate", 2).alias("Win Rate %"),
        F.round("frequency", 2).alias("Frequency %"),
        "games_with_unit",
        "popular_items",
    )
    .orderBy("tier", "Avg Place")
)

print(">>> Top 30 Units:")
unit_stats_clean.show(30, truncate=False)

# Xuất bảng PNG giống item tier list
pdf = unit_stats_clean.toPandas()
pdf["Popular Items"] = pdf["popular_items"].apply(
    lambda x: ", ".join(x[:5]) if isinstance(x, list) else ""
)
pdf = pdf.drop(columns=["popular_items"])
pdf.insert(0, "#", range(1, len(pdf) + 1))

plt.figure(figsize=(22, max(8, len(pdf) * 0.35)))
plt.axis("off")
table = plt.table(
    cellText=pdf.values,
    colLabels=pdf.columns,
    cellLoc="left",
    loc="center",
)
table.auto_set_font_size(False)
table.set_fontsize(9)
table.scale(1, 1.6)
plt.title("TFT Unit Tier List", fontsize=14, fontweight="bold", pad=20)
plt.tight_layout()
output_file = "tft_unit_tier_list.png"
plt.savefig(output_file, dpi=150, bbox_inches="tight")
plt.close()
print(f">>> Đã lưu bảng: {output_file}")

spark.stop()
