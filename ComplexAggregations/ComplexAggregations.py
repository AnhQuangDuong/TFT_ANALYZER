import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, avg, count, rank, when, lit, concat_ws
from pyspark.sql.window import Window
import shutil


# Tai winutils cho Windows
#    - Truy cap: https://github.com/steveloughran/winutils
#    - Chon phien ban Hadoop (hadoop-3.0.0)
#    - Tai 2 file: winutils.exe va hadoop.dll
#    - Copy vao: C:\hadoop\bin\
#

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

current_dir = os.path.dirname(os.path.abspath(__file__))

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

# Tinh tong hop traits cho moi nguoi choi
trait_summary = traits.groupBy("puuid").agg(
    count("trait_name").alias("num_traits"),
    avg("tier_current").alias("avg_tier")
)

# Join va tinh toan
players_summary = players.join(trait_summary, "puuid", "left")

# Xep hang theo damage trong moi tran
window = Window.partitionBy("match_id").orderBy(col("damage").desc())
players_ranked = players_summary.withColumn("rank_by_damage", rank().over(window))

# Tinh hieu suat doi hinh
players_final = players_ranked.withColumn(
    "team_efficiency",
    when(col("num_traits") > 0, col("damage") / (col("level") * col("num_traits")))
    .otherwise(lit(None))
)

players_final.select(
    "name", "placement", "damage", "num_traits", "avg_tier", "team_efficiency"
).show(10, truncate=False)

# FILE 1: Thong tin tong quan nguoi choi
print("\nDang xuat FILE 1: Players Summary...")
players_summary_df = players_final.drop("traits", "units")

output_players = os.path.join(current_dir, "output", "players_summary")
if os.path.exists(output_players):
    shutil.rmtree(output_players)

players_summary_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_players)

for file in os.listdir(output_players):
    if file.endswith(".csv"):
        old_path = os.path.join(output_players, file)
        new_path = os.path.join(output_players, "players_summary.csv")
        shutil.move(old_path, new_path)
        print(f"OK Players Summary: {new_path}")
        break

# FILE 2: Chi tiet traits cua tung nguoi
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
if os.path.exists(output_traits):
    shutil.rmtree(output_traits)

traits_detail.coalesce(1).write.mode("overwrite").option("header", True).csv(output_traits)

# Doi ten file CSV
for file in os.listdir(output_traits):
    if file.endswith(".csv"):
        old_path = os.path.join(output_traits, file)
        new_path = os.path.join(output_traits, "traits_detail.csv")
        shutil.move(old_path, new_path)
        print(f"OK Traits Detail: {new_path}")
        break

# FILE 3: Chi tiet units cua tung nguoi
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
if os.path.exists(output_units):
    shutil.rmtree(output_units)

units_detail.coalesce(1).write.mode("overwrite").option("header", True).csv(output_units)

for file in os.listdir(output_units):
    if file.endswith(".csv"):
        old_path = os.path.join(output_units, file)
        new_path = os.path.join(output_units, "units_detail.csv")
        shutil.move(old_path, new_path)
        print(f"OK Units Detail: {new_path}")
        break

# Tong ket
print("\n" + "="*60)
print("DA XUAT XONG 3 FILE CSV:")
print("="*60)
print(f"1. Players Summary  -> {output_players}\\players_summary.csv")
print(f"2. Traits Detail    -> {output_traits}\\traits_detail.csv")
print(f"3. Units Detail     -> {output_units}\\units_detail.csv")
print("="*60)
print(f"\nSo luong dong:")
print(f"   - Players: {players_summary_df.count()} dong")
print(f"   - Traits:  {traits_detail.count()} dong")
print(f"   - Units:   {units_detail.count()} dong")

spark.stop()
