from pyspark.sql import SparkSession
from pyspark.sql.types import *

# === Schema ===
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

# === Spark Session ===
spark = (
    SparkSession.builder
    .appName("TFT-MatchData-Processor")
    .master("local[*]")
    .config("spark.driver.memory", "8g")
    .config("spark.hadoop.fs.file.impl.disable.cache", "true")
    .config("spark.hadoop.fs.checksum.disabled", "true")
    .config("spark.hadoop.validateOutputSpecs", "false")
    .config("spark.hadoop.fs.local.impl", "org.apache.hadoop.fs.RawLocalFileSystem")  # ✅ Key fix
    .getOrCreate()
)

# === Read Folder ===
df = spark.read.json("/tmp/data/data_3580_matches", schema=match_schema)


df.printSchema()
df.show(2, truncate=False)


# Explode participants → units → items
from pyspark.sql import functions as F

traits_df = (
    df
    .withColumn("p", F.explode("info.participants"))
    .select(
        F.col("p.puuid").alias("puuid"),
        F.col("p.placement").alias("placement"),
        F.col("p.win").alias("win"),
        F.expr("transform(p.traits, x -> x.name)").alias("trait_list")
    )
)
traits_df = traits_df.withColumn("trait_comp",
    F.concat_ws("+", F.array_sort("trait_list"))
)
comp_stats = (
    traits_df
    .groupBy("trait_comp")
    .agg(
        F.count("*").alias("games"),
        F.avg("win".cast("int")).alias("win_rate"),
        F.avg("placement").alias("avg_placement")
    )
    .filter("games >= 50")  # Optional: ignore low-sample comps
    .orderBy(
        F.desc("win_rate"),
        F.asc("avg_placement")
    )
)
comp_stats.show(20, truncate=False)

traits_df = (
    df
    .withColumn("p", F.explode("info.participants"))
    .select(
        "p.win", "p.placement",
        F.expr("filter(p.traits, x -> x.num_units > 0) as active_traits")
    )
    .withColumn("trait_list",
        F.expr("transform(active_traits, x -> x.name)")
    )
)

traits_df = traits_df.withColumn("trait_comp",
    F.concat_ws("+", F.array_sort("trait_list"))
)