import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, avg, count, rank, when, lit, concat_ws,
    row_number, dense_rank, lag, lead, ntile, percent_rank,
    collect_list, collect_set, first, last, stddev, variance,
    max, min, sum, countDistinct
)
from pyspark.sql.window import Window
import shutil
import time


# Tai winutils cho Windows
#    - Truy cap: https://github.com/steveloughran/winutils
#    - Chon phien ban Hadoop (hadoop-3.0.0)
#    - Tai 2 file: winutils.exe va hadoop.dll
#    - Copy vao: C:\hadoop\bin\
#

# Setup Hadoop cho Windows
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin" + os.pathsep + os.environ.get("PATH", "")

# Tao thu muc tam neu chua co
os.makedirs("C:\\tmp", exist_ok=True)
os.makedirs("C:\\hadoop\\bin", exist_ok=True)

# Tao SparkSession
spark = (
    SparkSession.builder
    .appName("TFT_Analyzer")
    .master("local[*]")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    .config("spark.hadoop.io.nativeio.enabled", "false")
    .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse")
    .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=C:/tmp")
    .config("spark.executor.extraJavaOptions", "-Djava.io.tmpdir=C:/tmp")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Ham xoa thu muc an toan
def safe_rmtree(path, max_retries=3):
    """Xoa thu muc an toan, thu lai neu gap loi"""
    for i in range(max_retries):
        try:
            if os.path.exists(path):
                shutil.rmtree(path)
            return True
        except PermissionError:
            if i < max_retries - 1:
                print(f"Dang thu lai... ({i+1}/{max_retries})")
                time.sleep(1)
            else:
                print(f"CANH BAO: Khong the xoa {path}. Vui long dong cac file dang mo trong Excel!")
                return False
    return False

# Duong dan tuong doi
current_dir = os.path.dirname(os.path.abspath(__file__))

# Doc du lieu JSON
input_path = os.path.join(current_dir, "part-00000-a47f7a7f-88b7-48de-8129-7384dc4d7249-c000.json")
df = spark.read.option("multiline", True).json(input_path)
print("=== Schema ===")
df.printSchema()

# Flatten du lieu nguoi choi
players = df.select(
    col("data.metadata.match_id").alias("match_id"),
    explode(col("data.info.participants")).alias("p")
).select(
    "match_id",
    col("p.puuid").alias("puuid"),
    col("p.riotIdGameName").alias("name"),
    col("p.placement").alias("placement"),
    col("p.level").alias("level"),
    col("p.gold_left").alias("gold_left"),
    col("p.total_damage_to_players").alias("damage"),
    col("p.traits").alias("traits"),
    col("p.units").alias("units")
)
players.show(3, truncate=False)

# Phan tich traits
traits = players.select(
    "match_id", "puuid", "name", explode("traits").alias("t")
).select(
    "match_id", "puuid", "name",
    col("t.name").alias("trait_name"),
    col("t.tier_current").alias("tier_current"),
    col("t.num_units").alias("num_units")
)

# ==============================================================================
# 1. WINDOW FUNCTIONS - Day du cac loai
# ==============================================================================
print("\n=== 1. WINDOW FUNCTIONS ===")

# Dinh nghia cac window
window_damage = Window.partitionBy("match_id").orderBy(col("damage").desc())
window_level = Window.partitionBy("match_id").orderBy(col("level").desc())
window_all = Window.partitionBy("match_id")

# Ap dung tat ca cac window functions
players_with_windows = players.withColumn("rank_damage", rank().over(window_damage)) \
    .withColumn("dense_rank_damage", dense_rank().over(window_damage)) \
    .withColumn("row_number_damage", row_number().over(window_damage)) \
    .withColumn("percent_rank_damage", percent_rank().over(window_damage)) \
    .withColumn("ntile_4", ntile(4).over(window_damage)) \
    .withColumn("lag_damage", lag("damage", 1).over(window_damage)) \
    .withColumn("lead_damage", lead("damage", 1).over(window_damage))

players_with_windows.select(
    "name", "damage", "rank_damage", "dense_rank_damage", 
    "row_number_damage", "ntile_4", "lag_damage", "lead_damage"
).show(10, truncate=False)

# ==============================================================================
# 2. AGGREGATION FUNCTIONS - Nang cao
# ==============================================================================
print("\n=== 2. AGGREGATION FUNCTIONS ===")

# Tinh tong hop traits cho moi nguoi choi voi nhieu metrics
trait_summary = traits.groupBy("puuid").agg(
    count("trait_name").alias("num_traits"),
    avg("tier_current").alias("avg_tier"),
    stddev("tier_current").alias("stddev_tier"),
    variance("tier_current").alias("variance_tier"),
    max("tier_current").alias("max_tier"),
    min("tier_current").alias("min_tier"),
    sum("num_units").alias("total_units"),
    collect_list("trait_name").alias("all_traits"),
    collect_set("trait_name").alias("unique_traits"),
    first("trait_name").alias("first_trait"),
    last("trait_name").alias("last_trait"),
    countDistinct("trait_name").alias("distinct_traits")
)

trait_summary.show(5, truncate=False)

# Tinh tong hop cho tung match
match_summary = players.groupBy("match_id").agg(
    count("puuid").alias("total_players"),
    avg("damage").alias("avg_damage"),
    stddev("damage").alias("stddev_damage"),
    max("damage").alias("max_damage"),
    min("damage").alias("min_damage"),
    avg("level").alias("avg_level"),
    sum("gold_left").alias("total_gold_left")
)

match_summary.show(truncate=False)

# ==============================================================================
# 3. PIVOT - Chuyen traits thanh cot
# ==============================================================================
print("\n=== 3. PIVOT ===")

# Pivot: Moi trait thanh mot cot, gia tri la tier_current
traits_pivot = traits.groupBy("match_id", "puuid", "name").pivot("trait_name").agg(
    first("tier_current")
)

traits_pivot.show(5, truncate=False)

# ==============================================================================
# 4. UNPIVOT - Chuyen cot thanh dong (dung stack)
# ==============================================================================
print("\n=== 4. UNPIVOT ===")

# Lay danh sach tat ca cac trait columns (tru cac cot khoa)
trait_columns = [c for c in traits_pivot.columns if c not in ["match_id", "puuid", "name"]]

# Tao unpivot expression
unpivot_expr = "stack({}, {}) as (trait_name, tier_current)".format(
    len(trait_columns),
    ", ".join([f"'{c}', `{c}`" for c in trait_columns])
)

# Thuc hien unpivot
traits_unpivot = traits_pivot.selectExpr("match_id", "puuid", "name", unpivot_expr) \
    .filter(col("tier_current").isNotNull())

traits_unpivot.show(10, truncate=False)

# ==============================================================================
# 5. CUSTOM AGGREGATION FUNCTION - Phuc tap hon
# ==============================================================================
print("\n=== 5. CUSTOM AGGREGATION ===")

# Join va tinh toan
players_summary = players.join(trait_summary, "puuid", "left")

window = Window.partitionBy("match_id").orderBy(col("damage").desc())
players_ranked = players_summary.withColumn("rank_by_damage", rank().over(window))

# Custom aggregations phuc tap
players_final = players_ranked \
    .withColumn(
        "team_efficiency",
        when(col("num_traits") > 0, col("damage") / (col("level") * col("num_traits")))
        .otherwise(lit(None))
    ) \
    .withColumn(
        "damage_per_level",
        col("damage") / col("level")
    ) \
    .withColumn(
        "performance_score",
        (col("damage") * 0.4 + (9 - col("placement")) * 1000 + col("level") * 100) / 100
    ) \
    .withColumn(
        "efficiency_rank",
        when(col("team_efficiency").isNotNull(), 
             rank().over(Window.partitionBy("match_id").orderBy(col("team_efficiency").desc())))
        .otherwise(lit(None))
    )

players_final.select(
    "name", "placement", "damage", "num_traits", "avg_tier", 
    "team_efficiency", "damage_per_level", "performance_score", "efficiency_rank"
).show(10, truncate=False)

# ==============================================================================
# FILE 1: Thong tin tong quan nguoi choi (Voi Window Functions)
# ==============================================================================
print("\nDang xuat FILE 1: Players Summary...")
players_summary_df = players_final.drop("traits", "units", "all_traits", "unique_traits")

output_players = os.path.join(current_dir, "output", "players_summary")
safe_rmtree(output_players)

players_summary_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_players)

# Doi ten file CSV cho de doc
for file in os.listdir(output_players):
    if file.endswith(".csv"):
        old_path = os.path.join(output_players, file)
        new_path = os.path.join(output_players, "players_summary.csv")
        if os.path.exists(new_path):
            os.remove(new_path)
        shutil.move(old_path, new_path)
        print(f"OK Players Summary: {new_path}")
        break

# ==============================================================================
# FILE 2: Chi tiet traits cua tung nguoi
# ==============================================================================
print("\nDang xuat FILE 2: Traits Detail...")
traits_detail = players.select(
    "match_id", "puuid", "name", "placement",
    explode("traits").alias("trait")
).select(
    "match_id", "puuid", "name", "placement",
    col("trait.name").alias("trait_name"),
    col("trait.num_units").alias("num_units"),
    col("trait.tier_current").alias("tier_current"),
    col("trait.tier_total").alias("tier_total")
)

output_traits = os.path.join(current_dir, "output", "traits_detail")
safe_rmtree(output_traits)

traits_detail.coalesce(1).write.mode("overwrite").option("header", True).csv(output_traits)

# Doi ten file CSV
for file in os.listdir(output_traits):
    if file.endswith(".csv"):
        old_path = os.path.join(output_traits, file)
        new_path = os.path.join(output_traits, "traits_detail.csv")
        if os.path.exists(new_path):
            os.remove(new_path)
        shutil.move(old_path, new_path)
        print(f"OK Traits Detail: {new_path}")
        break

# ==============================================================================
# FILE 3: Chi tiet units cua tung nguoi
# ==============================================================================
print("\nDang xuat FILE 3: Units Detail...")
units_detail = players.select(
    "match_id", "puuid", "name", "placement",
    explode("units").alias("unit")
).select(
    "match_id", "puuid", "name", "placement",
    col("unit.character_id").alias("champion"),
    col("unit.tier").alias("tier"),
    col("unit.rarity").alias("rarity"),
    concat_ws("|", col("unit.itemNames")).alias("items")
)

output_units = os.path.join(current_dir, "output", "units_detail")
safe_rmtree(output_units)

units_detail.coalesce(1).write.mode("overwrite").option("header", True).csv(output_units)

# Doi ten file CSV
for file in os.listdir(output_units):
    if file.endswith(".csv"):
        old_path = os.path.join(output_units, file)
        new_path = os.path.join(output_units, "units_detail.csv")
        if os.path.exists(new_path):
            os.remove(new_path)
        shutil.move(old_path, new_path)
        print(f"OK Units Detail: {new_path}")
        break

# ==============================================================================
# FILE 4: Traits Pivot (Bonus)
# ==============================================================================
print("\nDang xuat FILE 4: Traits Pivot...")
output_pivot = os.path.join(current_dir, "output", "traits_pivot")
safe_rmtree(output_pivot)

traits_pivot.coalesce(1).write.mode("overwrite").option("header", True).csv(output_pivot)

for file in os.listdir(output_pivot):
    if file.endswith(".csv"):
        old_path = os.path.join(output_pivot, file)
        new_path = os.path.join(output_pivot, "traits_pivot.csv")
        if os.path.exists(new_path):
            os.remove(new_path)
        shutil.move(old_path, new_path)
        print(f"OK Traits Pivot: {new_path}")
        break

# ==============================================================================
# FILE 5: Match Summary (Aggregations)
# ==============================================================================
print("\nDang xuat FILE 5: Match Summary...")
output_match = os.path.join(current_dir, "output", "match_summary")
safe_rmtree(output_match)

match_summary.coalesce(1).write.mode("overwrite").option("header", True).csv(output_match)

for file in os.listdir(output_match):
    if file.endswith(".csv"):
        old_path = os.path.join(output_match, file)
        new_path = os.path.join(output_match, "match_summary.csv")
        if os.path.exists(new_path):
            os.remove(new_path)
        shutil.move(old_path, new_path)
        print(f"OK Match Summary: {new_path}")
        break

# ==============================================================================
# Tong ket
# ==============================================================================
print("\n" + "="*60)
print("DA XUAT XONG 5 FILE CSV:")
print("="*60)
print(f"1. Players Summary   -> {output_players}\\players_summary.csv")
print(f"2. Traits Detail     -> {output_traits}\\traits_detail.csv")
print(f"3. Units Detail      -> {output_units}\\units_detail.csv")
print(f"4. Traits Pivot      -> {output_pivot}\\traits_pivot.csv")
print(f"5. Match Summary     -> {output_match}\\match_summary.csv")
print("="*60)
print(f"\nSo luong dong:")
print(f"   - Players: {players_summary_df.count()} dong")
print(f"   - Traits:  {traits_detail.count()} dong")
print(f"   - Units:   {units_detail.count()} dong")
print(f"   - Pivot:   {traits_pivot.count()} dong")
print(f"   - Match:   {match_summary.count()} dong")

print("\n=== CAC KY THUAT DA SU DUNG ===")
print("1. Window Functions: rank, dense_rank, row_number, percent_rank, ntile, lag, lead")
print("2. Aggregations: count, avg, stddev, variance, max, min, sum, collect_list, collect_set, first, last, countDistinct")
print("3. Pivot: Chuyen traits thanh cot")
print("4. Unpivot: Chuyen cot thanh dong (dung stack)")
print("5. Custom Aggregations: team_efficiency, damage_per_level, performance_score")

spark.stop()