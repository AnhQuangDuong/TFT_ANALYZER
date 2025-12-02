        # ...existing code...
        from pyspark.sql import SparkSession, functions as F
        from pyspark.sql.types import *
        import os
        import sys

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
            .config("spark.hadoop.fs.local.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
            .getOrCreate()
        )

        # === Read Folder (project-relative) ===
        data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data_3580_matches"))
        if not os.path.exists(data_dir):
            raise FileNotFoundError(f"Data folder not found: {data_dir}")

        df = spark.read.json(data_dir, schema=match_schema)

        # === Compute comp-level statistics (avg placement, pick rate, top1/top4 rates) ===

        # total participant count (used for pick_rate)
        total_row = df.select(F.size("info.participants").alias("p")) \
            .agg(F.sum("p").alias("total_p")).collect()
        total_participants = 0
        if total_row and total_row[0] and total_row[0]["total_p"] is not None:
            total_participants = int(total_row[0]["total_p"])

        if total_participants == 0:
            spark.stop()
            raise RuntimeError(f"total_participants == 0, check data path: {data_dir}")

        # flatten participants and build canonical comp signature from unit character ids
        # strip any "TFT<number>_" prefix (handles different sets), dedupe and sort unit ids
        flattened = (
            df
            .withColumn("p", F.explode("info.participants"))
            .select(
                F.col("p.puuid").alias("puuid"),
                F.col("p.placement").alias("placement"),
                F.col("p.win").alias("win"),
                F.expr("transform(p.units, x -> regexp_replace(x.character_id, 'TFT\\\\d+_', ''))").alias("unit_ids")
            )
            .withColumn("unit_ids", F.array_distinct(F.col("unit_ids")))
            .withColumn("unit_ids", F.array_sort(F.col("unit_ids")))
            # drop participants with no units (avoid empty comp signatures)
            .filter(F.size("unit_ids") > 0)
            .withColumn("comp_sig", F.concat_ws("+", F.col("unit_ids")))
        )

        # aggregate per comp_sig
        comp_stats = (
            flattened
            .groupBy("comp_sig")
            .agg(
                F.count("*").alias("games"),
                F.round(F.avg("placement"), 3).alias("avg_placement"),
                F.round(F.avg(F.col("win").cast("int")), 4).alias("win_rate"),
                F.sum(F.when(F.col("placement") == 1, 1).otherwise(0)).alias("top1_count"),
                F.sum(F.when(F.col("placement") <= 4, 1).otherwise(0)).alias("top4_count")
            )
            .withColumn("pick_rate", F.round(F.col("games") / F.lit(total_participants) * 100, 3))
            .withColumn("top1_rate", F.round(F.col("top1_count") / F.col("games") * 100, 3))
            .withColumn("top4_rate", F.round(F.col("top4_count") / F.col("games") * 100, 3))
            .drop("top1_count", "top4_count")
        )

        # optional: filter low-sample comps and order (threshold can be adjusted)
        MIN_GAMES = 50
        result = (
            comp_stats
            .filter(F.col("games") >= MIN_GAMES)
            .orderBy(F.desc("win_rate"), F.asc("avg_placement"), F.desc("pick_rate"))
        )

        # show top results
        result.show(100, truncate=False)

        # stop spark
        spark.stop()