from pyspark.sql import SparkSession

from pyspark.sql.types import *

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
        StructField("game_variation", StringType()),
        StructField("mapId", IntegerType()),
        StructField("participants", ArrayType(StructType([
            StructField("companion", StructType([
                StructField("content_ID", StringType()),
                StructField("item_ID", IntegerType()),
                StructField("skin_ID", IntegerType()),
                StructField("species", StringType())
            ])),
            StructField("gold_left", IntegerType()),
            StructField("last_round", IntegerType()),
            StructField("level", IntegerType()),
            StructField("placement", IntegerType()),
            StructField("players_eliminated", IntegerType()),
            StructField("puuid", StringType()),
            StructField("riotIdGameName", StringType()),
            StructField("riotIdTagline", StringType()),
            StructField("time_eliminated", DoubleType()),
            StructField("total_damage_to_players", IntegerType()),
            StructField("win", BooleanType()),
            StructField("traits", ArrayType(StructType([
                StructField("name", StringType()),
                StructField("num_units", IntegerType()),
                StructField("style", IntegerType()),
                StructField("tier_current", IntegerType()),
                StructField("tier_total", IntegerType())
            ]))),
            StructField("units", ArrayType(StructType([
                StructField("items", ArrayType(IntegerType())),
                StructField("character_id", StringType()),
                StructField("itemNames", ArrayType(StringType())),
                StructField("chosen", StringType()),
                StructField("name", StringType()),
                StructField("rarity", IntegerType()),
                StructField("tier", IntegerType())
            ])))
        ]))),
        StructField("queue_id", IntegerType()),
        StructField("queueId", IntegerType()),
        StructField("tft_game_type", StringType()),
        StructField("tft_set_core_name", StringType()),
        StructField("tft_set_number", IntegerType())
    ]))
])


spark = SparkSession.builder \
    .appName("TFT-MatchData-Processor") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.json(r"TFT_ANALYZER\data_3580_matches\part-00000-a47f7a7f-88b7-48de-8129-7384dc4d7249-c000.json", schema=match_schema)
df.printSchema()
df.show(2, truncate=False)
