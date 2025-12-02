from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from dotenv import load_dotenv
import os

load_dotenv()
#day_crawl = os.getenv("DAY_CRAWL")
day_crawl = "2025-11-25"

# run first time to download the spark mongodb package
# spark = SparkSession.builder.appName("ReadFromHDFS").config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/tft_db.compositions") \
#     .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
#     .getOrCreate()

spark = SparkSession.builder \
    .appName("ProcessCompsToMongoDB") \
    .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/tft_db.compositions") \
    .config("spark.jars", "/home/anhdq/.ivy2/jars/org.mongodb.spark_mongo-spark-connector_2.12-10.3.0.jar,/home/anhdq/.ivy2/jars/org.mongodb_mongodb-driver-sync-4.8.2.jar,/home/anhdq/.ivy2/jars/org.mongodb_bson-4.8.2.jar,/home/anhdq/.ivy2/jars/org.mongodb_mongodb-driver-core-4.8.2.jar") \
    .getOrCreate()

# Read Parquet files from HDFS
df = spark.read.parquet(f"hdfs://192.168.200.128:9000/tft/{day_crawl}/stream_output")

# loc cac tran trung nhau
df = df.withColumn("match_id", F.col("data.metadata.match_id")).dropDuplicates(["match_id"]).drop("match_id")
    
print("Total matches loaded from HDFS:", df.count())

# Show the data
#df.show()

df = df.filter(F.col("data.info.tft_game_type") == "standard")
# trong cot game_type co 3 gia tri unique: "standard", "pairs", "pve"

# can phai filter them tft_set_core_name ="TFTSet15" do co nhieu set khac nhau vi du tai hien mua 7
df = df.filter(F.col("data.info.tft_set_core_name") == "TFTSet15")

exploded = df.select(
    F.col("data.metadata.match_id").alias("match_id"),
    F.col("data.info.player_rank").alias("player_rank"),
    F.posexplode("data.info.participants").alias("participant_index", "participant")
)

# in ra cac player_rank unique, neu rank khac nhau co the them window function de danh gia phan bo doi hinh theo rank
unique_ranks = exploded.select("player_rank").distinct().orderBy("player_rank")
print("Unique player ranks:")
unique_ranks.show()

# loc cac match_id trung nhau
exploded = exploded.dropDuplicates(["match_id"])
total_matches = exploded.count()
print("number of standard games:", total_matches)

#exploded.show(5)

flattened = exploded.select(
    "match_id",
    "player_rank",
    F.col("participant.placement").alias("placement"),
    F.col("participant.level").alias("level"),
    F.col("participant.units").alias("units")
)
#flattened.show(5)

# Tao UDF de tao signature cho mot doi hinh
@F.udf(returnType=T.StringType())
def create_comp_sig(units):
    unit_list = []
    for unit in units:
        #print(unit)
        unit_list.append(unit['character_id'].replace("TFT15_","").replace("tft15_",""))
    unit_list.sort()
    return "|".join(unit_list)

# Tao UDF de tim ra cac tuong core trong doi hinh
@F.udf(returnType=T.ArrayType(
        T.StructType([
            T.StructField("character_id", T.StringType(), True),
            T.StructField("itemNames", T.ArrayType(T.StringType()), True),
            T.StructField("tier", T.IntegerType(), True)
        ])
))
def find_core_units(units):
    core_units = []
    for unit in units:
        if unit['itemNames']:
            core_unit = {
                'character_id': unit['character_id'],
                'itemNames': unit['itemNames'],
                'tier': unit['tier']
            }
            core_units.append(core_unit)
    return core_units

flattened_with_sig_and_core_units = flattened.withColumn(
    "comp_sig", create_comp_sig(F.col("units"))
).withColumn(
    "core_units", find_core_units(F.col("units"))
)

flattened_with_sig_and_core_units.show(5)

# tao ham tim ra top 4 tuong carry xuat hien nhieu nhat trong core_units cua moi comp_sig
@F.udf(returnType=T.ArrayType(T.StringType()))
def find_top_4_carry_from_collected(all_core_units):
    carry_count = {}
    
    for core_units_row in all_core_units:
        if core_units_row:
            for unit in core_units_row:
                character_id = unit['character_id'].replace("TFT15_", "").replace("tft15_", "")
                if character_id in carry_count:
                    carry_count[character_id] += 1
                else:
                    carry_count[character_id] = 1
    
    # Sắp xếp giảm dần theo số lần xuất hiện
    sorted_carry = sorted(carry_count.items(), key=lambda x: x[1], reverse=True)
    top_4_carry = [item[0] for item in sorted_carry[:4]]
    return top_4_carry

# Tim ra average placement cho moi comp_sig, pick rate, top 4 tuong carry cho moi comp_sig
avg_placement_df = flattened_with_sig_and_core_units.groupBy("comp_sig").agg(
    F.avg("placement").alias("avg_placement"),
    F.count("comp_sig").alias("num_matches"),
    F.collect_list("core_units").alias("all_core_units")
).withColumn(
    "top_4_carry", find_top_4_carry_from_collected(F.col("all_core_units"))
).drop("all_core_units")

# Phân tích placement distribution theo comp_sig
placement_pivot = flattened_with_sig_and_core_units.groupBy("comp_sig").pivot(
    "placement", [1, 2, 3, 4, 5, 6, 7, 8]
).count().fillna(0)

# Tinh top 4 rate
placement_stats = placement_pivot.withColumn(
    "top4_rate", F.round((F.col("1") + F.col("2") + F.col("3") + F.col("4")) / (F.col("1") + F.col("2") + F.col("3") + F.col("4") + F.col("5") + F.col("6") + F.col("7") + F.col("8")) * 100, 2)
)

avg_placement_and_pick_rate_df = avg_placement_df.withColumn(
    "pick_rate", F.round(F.col("num_matches") / total_matches * 100, 2)
).drop("num_matches")

# join avg_placement_and_pick_rate_df va top4_rate
avg_placement_and_pick_rate_df_and_top4rate = avg_placement_and_pick_rate_df.join(
    placement_stats.select("comp_sig", "top4_rate"),
    on="comp_sig",
    how="left"
).orderBy(F.col("pick_rate").desc())

avg_placement_and_pick_rate_df_and_top4rate = avg_placement_and_pick_rate_df_and_top4rate.filter(F.col("avg_placement") < 4.5)

avg_placement_and_pick_rate_df_and_top4rate.show(20)

# Ghi DataFrame vào MongoDB
avg_placement_and_pick_rate_df_and_top4rate.write.format("mongodb").mode("overwrite").option("database", "tft_db").option("collection", "compositions").save()

print(">>> Successfully wrote compositions data to MongoDB (tft_db.compositions)")

spark.stop()
# Optionally, print schema
#df.printSchema()

# Export data to JSON file (local path)
#df.coalesce(1).write.mode("overwrite").json("file:///home/anhdq/bigdata_project/data_3580_matches")