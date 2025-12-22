import os
import time
from datetime import datetime
from collections import defaultdict

import pytz
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col, explode

# ========= Config from environment =========
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "192.168.200.128:9093")
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://192.168.200.128:27017/")
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "hdfs://192.168.200.128:9000")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "match_history")
BATCH_TRIGGER_SECONDS = os.getenv("BATCH_TRIGGER_SECONDS", "30")

# ========= MongoDB setup =========
mongo_client = MongoClient(MONGODB_URI)
speed_db = mongo_client["tft_db"]
player_speed_collection = speed_db["players_speed"]


def process_speed_layer(batch_df, batch_id):
    """Process micro-batch for speed layer analytics and upsert to MongoDB."""
    try:
        if batch_df.count() == 0:
            return

        exploded_matches = batch_df.select(
            col("player_name"),
            col("player_tag"),
            col("player_rank"),
            explode(col("matches")).alias("match_data"),
        )

        total_matches = exploded_matches.count()
        if total_matches == 0:
            return

        player_matches = (
            exploded_matches.select(
                col("player_name"),
                col("player_tag"),
                col("player_rank"),
                col("match_data.metadata.match_id").alias("match_id"),
                col("match_data.info.game_datetime").alias("game_datetime"),
                explode(col("match_data.info.participants")).alias("participant"),
            )
            .filter(
                (col("participant.riotIdGameName") == col("player_name"))
                & (col("participant.riotIdTagline") == col("player_tag"))
            )
            .select(
                col("player_name"),
                col("player_tag"),
                col("player_rank"),
                col("match_id"),
                col("game_datetime"),
                col("participant.placement").alias("placement"),
                col("participant.traits").alias("traits"),
                col("participant.units").alias("units"),
            )
        )

        players_data = player_matches.collect()
        players_stats = defaultdict(lambda: {"player_rank": None, "placements": []})

        for row in players_data:
            player_key = f"{row['player_name']}#{row['player_tag']}"
            players_stats[player_key]["player_rank"] = row["player_rank"]
            players_stats[player_key]["placements"].append(row["placement"])

        player_documents = []
        vietnam_tz = pytz.timezone("Asia/Ho_Chi_Minh")

        for player_key, stats in players_stats.items():
            placements = stats["placements"]
            total_games = len(placements)
            if total_games == 0:
                continue

            placement_distribution = {str(i): placements.count(i) for i in range(1, 9)}
            avg_placement = sum(placements) / total_games
            top4_count = sum(1 for p in placements if p <= 4)
            top4_rate = (top4_count / total_games * 100) if total_games else 0

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
                "source": "speed_layer",
            }
            player_documents.append(doc)

        for doc in player_documents:
            player_speed_collection.replace_one({"_id": doc["_id"]}, doc, upsert=True)

        print(f"✅ Speed Layer batch {batch_id}: {len(player_documents)} players, {total_matches} matches")
    except Exception as exc:  # pragma: no cover - runtime logging
        print(f"❌ Error in speed layer batch {batch_id}: {exc}")
        import traceback
        traceback.print_exc()


def write_to_hdfs_batch(batch_df, batch_id):
    """Write individual matches to HDFS for batch layer."""
    try:
        if batch_df.count() == 0:
            return

        individual_matches = (
            batch_df.select(explode(col("matches")).alias("match"))
            .select(col("match.*"))
        )

        if individual_matches.count() > 0:
            vietnam_tz = pytz.timezone("Asia/Ho_Chi_Minh")
            current_date = datetime.now(vietnam_tz).date()
            target_path = f"{HDFS_NAMENODE}/tft/{current_date}/stream_output"
            individual_matches.write.mode("append").parquet(target_path)
            print(f"✅ Batch {batch_id}: Written {individual_matches.count()} matches to {target_path}")
    except Exception as exc:  # pragma: no cover - runtime logging
        print(f"❌ Error writing batch {batch_id} to HDFS: {exc}")
        import traceback
        traceback.print_exc()


# ========= Spark setup =========
LOCAL_JARS = [
    "/app/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar",
    "/app/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
    "/app/jars/kafka-clients-3.4.1.jar",
    "/app/jars/lz4-java-1.8.0.jar",
    "/app/jars/snappy-java-1.1.10.3.jar",
    "/app/jars/slf4j-api-2.0.7.jar",
    "/app/jars/commons-pool2-2.11.1.jar",
    "/app/jars/jsr305-3.0.0.jar",
    "/app/jars/commons-logging-1.1.3.jar",
    "/app/jars/hadoop-client-runtime-3.3.4.jar",
    "/app/jars/hadoop-client-api-3.3.4.jar",
]

spark = (
    SparkSession.builder
    .appName("KafkaToHDFS")
    .master("local[*]")
    .config("spark.jars", ",".join(LOCAL_JARS))
    .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
    .config("spark.hadoop.dfs.blocksize", "10485760")
    .getOrCreate()
)

match_schema = StructType([
    StructField("metadata", StructType([
        StructField("data_version", StringType()),
        StructField("match_id", StringType()),
        StructField("participants", ArrayType(StringType())),
    ])),
    StructField("info", StructType([
        StructField("endOfGameResult", StringType()),
        StructField("gameCreation", LongType()),
        StructField("gameId", LongType()),
        StructField("game_datetime", LongType()),
        StructField("game_length", DoubleType()),
        StructField("game_version", StringType()),
        StructField("mapId", IntegerType()),
        StructField("participants", ArrayType(StructType([
            StructField("companion", StructType([
                StructField("content_ID", StringType()),
                StructField("item_ID", IntegerType()),
                StructField("skin_ID", IntegerType()),
                StructField("species", StringType()),
            ])),
            StructField("gold_left", IntegerType()),
            StructField("last_round", IntegerType()),
            StructField("level", IntegerType()),
            StructField("missions", StructType([
                StructField("PlayerScore2", IntegerType()),
            ])),
            StructField("placement", IntegerType()),
            StructField("players_eliminated", IntegerType()),
            StructField("puuid", StringType()),
            StructField("riotIdGameName", StringType()),
            StructField("riotIdTagline", StringType()),
            StructField("time_eliminated", DoubleType()),
            StructField("total_damage_to_players", IntegerType()),
            StructField("traits", ArrayType(StructType([
                StructField("name", StringType()),
                StructField("num_units", IntegerType()),
                StructField("style", IntegerType()),
                StructField("tier_current", IntegerType()),
                StructField("tier_total", IntegerType()),
            ]))),
            StructField("units", ArrayType(StructType([
                StructField("character_id", StringType()),
                StructField("itemNames", ArrayType(StringType())),
                StructField("name", StringType()),
                StructField("rarity", IntegerType()),
                StructField("tier", IntegerType()),
            ]))),
        ]))),
        StructField("player_rank", StringType()),
        StructField("queueId", IntegerType()),
        StructField("queue_id", IntegerType()),
        StructField("tft_game_type", StringType()),
        StructField("tft_set_core_name", StringType()),
        StructField("tft_set_number", IntegerType()),
    ])),
])

kafka_record_schema = StructType([
    StructField("player_name", StringType()),
    StructField("player_tag", StringType()),
    StructField("player_rank", StringType()),
    StructField("matches", ArrayType(match_schema)),
])

df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed_df = (
    df_raw.selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), kafka_record_schema).alias("data"))
    .select(
        col("data.player_name").alias("player_name"),
        col("data.player_tag").alias("player_tag"),
        col("data.player_rank").alias("player_rank"),
        col("data.matches").alias("matches"),
    )
)


def process_batch(batch_df, batch_id):
    process_speed_layer(batch_df, batch_id)
    write_to_hdfs_batch(batch_df, batch_id)


query = (
    parsed_df.writeStream
    .foreachBatch(process_batch)
    .outputMode("append")
    .option(
        "checkpointLocation",
        f"{HDFS_NAMENODE}/tft/checkpoints",
    )
    .trigger(processingTime=f"{BATCH_TRIGGER_SECONDS} seconds")
    .start()
)

query.awaitTermination()
