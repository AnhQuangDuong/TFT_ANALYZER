from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col, explode, count, avg, round as spark_round
from pyspark.sql.window import Window
from pymongo import MongoClient
import time
from datetime import datetime
import pytz

# run first time to download the spark kafka package
#spark = SparkSession.builder.appName("TFT-MatchHistory-Stream").master("local[*]").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0").getOrCreate()

mongo_client = MongoClient("mongodb://localhost:27017/")
speed_db = mongo_client["tft_db"]
player_speed_collection = speed_db["players_speed"]

def process_speed_layer(batch_df, batch_id):
    """
    Xử lý real-time analytics từ micro-batch
    Phân tích phân bố xếp hạng và đội hình top >=4 của từng người chơi
    Lưu vào MongoDB Speed Collections
    """
    try:
        if batch_df.count() == 0:
            return
        
        # Explode list các trận để xử lý từng trận trong Speed Layer
        exploded_matches = batch_df.select(
            col("player_name"),
            col("player_tag"),
            col("player_rank"),
            explode(col("matches")).alias("match_data")
        )
        
        total_matches = exploded_matches.count()
        
        if total_matches == 0:
            return
        
        print(f"✅ Speed Layer batch {batch_id}: Processing {total_matches} matches from {batch_df.count()} players")
        
        # ==================== PHÂN TÍCH TỪNG NGƯỜI CHƠI ====================
        
        # Tìm thông tin của người chơi trong mỗi trận
        player_matches = exploded_matches.select(
            col("player_name"),
            col("player_tag"),
            col("player_rank"),
            col("match_data.metadata.match_id").alias("match_id"),
            col("match_data.info.game_datetime").alias("game_datetime"),
            explode(col("match_data.info.participants")).alias("participant")
        ).filter(
            # Lọc chỉ lấy thông tin của chính người chơi đó
            (col("participant.riotIdGameName") == col("player_name")) &
            (col("participant.riotIdTagline") == col("player_tag"))
        ).select(
            col("player_name"),
            col("player_tag"),
            col("player_rank"),
            col("match_id"),
            col("game_datetime"),
            col("participant.placement").alias("placement"),
            col("participant.traits").alias("traits"),
            col("participant.units").alias("units")
        )
        
        # Collect dữ liệu theo từng người chơi
        players_data = player_matches.collect()
        
        # Group theo player để phân tích
        from collections import defaultdict
        players_stats = defaultdict(lambda: {
            "player_rank": None,
            "matches": [],
            "placements": []
        })
        
        for row in players_data:
            player_key = f"{row['player_name']}#{row['player_tag']}"
            players_stats[player_key]["player_rank"] = row["player_rank"]
            players_stats[player_key]["placements"].append(row["placement"])
        
        # ==================== LƯU VÀO MONGODB ====================
        player_documents = []
        
        for player_key, stats in players_stats.items():
            placements = stats["placements"]
            
            # Tính phân bố xếp hạng
            placement_distribution = {
                "1st": placements.count(1),
                "2nd": placements.count(2),
                "3rd": placements.count(3),
                "4th": placements.count(4),
                "5th": placements.count(5),
                "6th": placements.count(6),
                "7th": placements.count(7),
                "8th": placements.count(8)
            }
            
            # Tính các metrics
            total_games = len(placements)
            avg_placement = sum(placements) / total_games if total_games > 0 else 0
            top4_count = sum(1 for p in placements if p <= 4)
            top4_rate = (top4_count / total_games * 100) if total_games > 0 else 0
            vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')

            doc = {
                "_id": f"player_{player_key}_{batch_id}",
                "player_name": player_key.split("#")[0],
                "player_tag": player_key.split("#")[1],
                "player_rank": stats["player_rank"],
                "total_games": total_games,
                "avg_placement": round(avg_placement, 2),
                "top4_count": top4_count,
                "top4_rate": round(top4_rate, 1),
                "placement_distribution": placement_distribution,
                "timestamp": datetime.now(pytz.UTC).astimezone(vietnam_tz),
                "batch_id": batch_id,
                "source": "speed_layer"
            }
            
            player_documents.append(doc)
        
        if player_documents:
            # Insert hoặc update vào MongoDB
            for doc in player_documents:
                player_speed_collection.replace_one(
                    {"_id": doc["_id"]},
                    doc,
                    upsert=True
                )
            
            print(f"✅ Speed Layer batch {batch_id}: Processed {len(player_documents)} players")
            print(f"   - Total matches analyzed: {total_matches}")
        
    except Exception as e:
        print(f"❌ Error in speed layer batch {batch_id}: {e}")
        import traceback
        traceback.print_exc()

def write_to_hdfs_batch(batch_df, batch_id):
    """
    Ghi từng trận riêng lẻ vào HDFS sau khi đã xử lý Speed Layer
    """
    try:
        if batch_df.count() == 0:
            return
        
        # Explode list các trận thành từng trận riêng lẻ
        individual_matches = batch_df.select(explode(col("matches")).alias("match")).select(col("match.*"))
        
        if individual_matches.count() > 0:
            vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')
            current_time = datetime.now(vietnam_tz)
            current_date = current_time.date()
            
            # Ghi từng trận vào HDFS
            individual_matches.write.mode("append").parquet(f"hdfs://192.168.200.128:9000/tft/{current_date}/stream_output")
            
            print(f"✅ Batch {batch_id}: Written {individual_matches.count()} matches to HDFS")
    
    except Exception as e:
        print(f"❌ Error writing batch {batch_id} to HDFS: {e}")
        import traceback
        traceback.print_exc()

spark = (
    SparkSession.builder
    .appName("KafkaToHDFS")
    .master("local[1]")
    .config(
        "spark.jars",
        ",".join([
            "/home/anhdq/.ivy2/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.5.0.jar",
            "/home/anhdq/.ivy2/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
            "/home/anhdq/.ivy2/jars/org.apache.kafka_kafka-clients-3.4.1.jar",
            "/home/anhdq/.ivy2/jars/org.lz4_lz4-java-1.8.0.jar",
            "/home/anhdq/.ivy2/jars/org.xerial.snappy_snappy-java-1.1.10.3.jar",
            "/home/anhdq/.ivy2/jars/org.slf4j_slf4j-api-2.0.7.jar",
            "/home/anhdq/.ivy2/jars/org.apache.commons_commons-pool2-2.11.1.jar",
            "/home/anhdq/.ivy2/jars/com.google.code.findbugs_jsr305-3.0.0.jar",
            "/home/anhdq/.ivy2/jars/commons-logging_commons-logging-1.1.3.jar",
            "/home/anhdq/.ivy2/jars/org.apache.hadoop_hadoop-client-runtime-3.3.4.jar",
            "/home/anhdq/.ivy2/jars/org.apache.hadoop_hadoop-client-api-3.3.4.jar",
        ])       
    )
    .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.200.128:9000")
    .config("spark.hadoop.dfs.blocksize", "10485760")
    .getOrCreate()
)

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

# schema cho record tu kafka broker (list tu producer)
kafka_record_schema = StructType([
    StructField("player_name", StringType()),
    StructField("player_tag", StringType()),
    StructField("player_rank", StringType()),
    StructField("matches", ArrayType(match_schema))
])

# .option("failOnDataLoss", "false"): chap nhan neu co data bi mat do qua trinh crawl hay bi chet server dan toi viec die kafka phai reset offset ve 0 khi khoi dong lai
df_raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "192.168.200.128:9093").option("subscribe", "match_history").option("startingOffsets", "earliest").option("failOnDataLoss", "false").load()

parsed_df = df_raw.selectExpr("CAST(value AS STRING) as json_str").select(from_json(col("json_str"), kafka_record_schema).alias("data")).select(
        col("data.player_name").alias("player_name"),
        col("data.player_tag").alias("player_tag"),
        col("data.player_rank").alias("player_rank"),
        col("data.matches").alias("matches")
    )

vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')
current_time = datetime.now(vietnam_tz)
current_date = current_time.date()
#current_date = "2025-11-25"

# Sử dụng foreachBatch để xử lý cả Speed Layer và ghi HDFS
def process_batch(batch_df, batch_id):
    # Xu ly speed layer va save vao mongodb
    process_speed_layer(batch_df, batch_id)
    
    # ghi tung tran vao HDFS
    write_to_hdfs_batch(batch_df, batch_id)

query = (
    parsed_df.writeStream
    .foreachBatch(process_batch)
    .outputMode("append")
    .option("checkpointLocation", f"hdfs://192.168.200.128:9000/tft/{current_date}/checkpoints")
    .trigger(processingTime="30 seconds")
    .start()
)

#parsed_df.printSchema()

query.awaitTermination(timeout = 500)