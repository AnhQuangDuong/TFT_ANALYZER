from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, explode, count, desc, avg

# 1. Khởi tạo Spark Session
spark = (SparkSession.builder
    .appName("TFT-Local-Analysis")
    .master("local[*]")  # Dùng hết sức mạnh CPU máy
    .config("spark.driver.memory", "4g") # Cấp ram cho driver nếu dữ liệu lớn
    .getOrCreate()
)

# 2. Định nghĩa Schema (Copy từ project của bạn để Spark hiểu cấu trúc lồng nhau)
match_schema = StructType([
    StructField("metadata", StructType([
        StructField("data_version", StringType()),
        StructField("match_id", StringType()),
        StructField("participants", ArrayType(StringType()))
    ])),
    StructField("info", StructType([
        StructField("endOfGameResult", StringType()),
        StructField("gameCreation", LongType()),
        StructField("gameId", LongType()),
        StructField("game_datetime", LongType()),
        StructField("game_length", DoubleType()),
        StructField("game_version", StringType()),
        StructField("mapId", IntegerType()),
        StructField("participants", ArrayType(
            StructType([
                StructField("companion", StructType([
                    StructField("content_ID", StringType()),
                    StructField("item_ID", IntegerType()),
                    StructField("skin_ID", IntegerType()),
                    StructField("species", StringType())
                ])),
                StructField("gold_left", IntegerType()),
                StructField("last_round", IntegerType()),
                StructField("level", IntegerType()),
                StructField("missions", StructType([
                    StructField("PlayerScore2", IntegerType())
                ])),
                StructField("placement", IntegerType()),
                StructField("players_eliminated", IntegerType()),
                StructField("puuid", StringType()),
                StructField("riotIdGameName", StringType()),
                StructField("riotIdTagline", StringType()),
                StructField("time_eliminated", DoubleType()),
                StructField("total_damage_to_players", IntegerType()),
                StructField("traits", ArrayType(
                    StructType([
                        StructField("name", StringType()),
                        StructField("num_units", IntegerType()),
                        StructField("style", IntegerType()),
                        StructField("tier_current", IntegerType()),
                        StructField("tier_total", IntegerType())
                    ])
                )),
                StructField("units", ArrayType(
                    StructType([
                        StructField("character_id", StringType()),
                        StructField("itemNames", ArrayType(StringType())),
                        StructField("name", StringType()),
                        StructField("rarity", IntegerType()),
                        StructField("tier", IntegerType())
                    ])
                ))
            ]))),
        StructField("player_rank", StringType()),
        StructField("queueId", IntegerType()),
        StructField("queue_id", IntegerType()),
        StructField("tft_game_type", StringType()),
        StructField("tft_set_core_name", StringType()),
        StructField("tft_set_number", IntegerType()),
    ]))
])

# 3. Đọc dữ liệu từ thư mục 'data_3580_matches'
print(">>> Đang đọc dữ liệu...")
# Lưu ý: Nếu dữ liệu trong folder là file output của Spark (như ảnh trước) thì dùng .json()
# Nếu dữ liệu là file json gốc chưa qua xử lý thì thêm option multiLine=True nếu cần
df = spark.read.schema(match_schema).json("./data_3580_matches")

# Cache dữ liệu vào RAM để chạy các câu lệnh sau nhanh hơn
df.cache()

print(f">>> Tổng số trận đấu tìm thấy: {df.count()}")

# ====================================================
# PHẦN THAO TÁC / ANALYTICS
# ====================================================

# VD1: Xem Top 5 trận đấu dài nhất
print("\n>>> Top 5 trận đấu dài nhất:")
df.select(
    col("info.gameId"),
    col("info.tft_game_type"),
    col("info.game_length")
).orderBy(desc("info.game_length")).show(5)

# VD2: Phân tích Tộc/Hệ (Traits) đang hot
# Cần dùng explode để 'bung' mảng participants -> bung tiếp mảng traits
print("\n>>> Top 10 Tộc/Hệ được kích hoạt nhiều nhất:")
df_traits = df.select(explode(col("info.participants")).alias("player")) \
              .select(explode(col("player.traits")).alias("trait"))

df_traits.groupBy("trait.name") \
    .agg(count("*").alias("count"), avg("trait.tier_current").alias("avg_tier")) \
    .orderBy(desc("count")) \
    .show(10, truncate=False)

# VD3: Phân tích Unit (Tướng)
print("\n>>> Top 10 Tướng xuất hiện nhiều nhất:")
df_units = df.select(explode(col("info.participants")).alias("player")) \
             .select(explode(col("player.units")).alias("unit"))

df_units.groupBy("unit.character_id") \
    .count() \
    .orderBy(desc("count")) \
    .show(10, truncate=False)

# Dừng session
spark.stop()