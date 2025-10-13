from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col

# run first time to download the spark kafka package
#spark = SparkSession.builder.appName("TFT-MatchHistory-Stream").master("local[*]").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0").getOrCreate()

spark = (
    SparkSession.builder
    .appName("TFT-MatchHistory-Stream")
    .master("local[*]")
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
                StructField("queueId", IntegerType()),
                StructField("queue_id", IntegerType()),
                StructField("tft_game_type", StringType()),
                StructField("tft_set_core_name", StringType()),
                StructField("tft_set_number", IntegerType()),
        ]))
    ])

df_raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "192.168.200.128:9093").option("subscribe", "match_history").option("startingOffsets", "earliest").load()

parsed_df = df_raw.selectExpr("CAST(value AS STRING) as json_str").select(from_json(col("json_str"), match_schema).alias("data"))

query = (
    parsed_df.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .start()
)

#parsed_df.printSchema()

query.awaitTermination()