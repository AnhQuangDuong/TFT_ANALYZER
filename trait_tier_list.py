from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, count, avg, desc, round as spark_round, 
    collect_list, concat, lit, row_number, sum as spark_sum, when, collect_set, concat_ws
)
from pyspark.sql.window import Window
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.table import Table
import matplotlib.patches as mpatches

# Khởi tạo Spark Session
spark = (SparkSession.builder
    .appName("TFT-Traits-Tier-List")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

# Đọc dữ liệu
print(">>> Đang đọc dữ liệu...")
df = spark.read.option("recursiveFileLookup", "true") \
               .option("inferSchema", "true") \
               .json("./data_3580_matches")

# Xử lý lớp bọc 'data' nếu có
if "data" in df.columns and "info" not in df.columns:
    df = df.select("data.*")

df.cache()
print(f"Done! Đã load {df.count()} trận đấu.")

# Bước 1: Phẳng hóa dữ liệu
df_players = df.select(
    explode(col("info.participants")).alias("player")
).select(
    col("player.placement"),
    col("player.traits")
)

# Bung mảng traits của mỗi người chơi
df_traits = df_players.select(
    col("placement"),
    explode(col("traits")).alias("trait")
).select(
    col("placement"),
    col("trait.name").alias("trait_id"),
    col("trait.num_units").alias("num_units"),
    col("trait.tier_current").alias("tier_current"),
    col("trait.tier_total").alias("tier_total"),
    col("trait.style").alias("style")
).filter(
    # Chỉ lấy những trait ĐÃ KÍCH HOẠT (tier_current > 0)
    col("tier_current") > 0
)

df_traits = df_traits.withColumn(
    "win", 
    (col("placement") <= 4).cast("int")
)

print(f"Tổng số traits đã kích hoạt tìm thấy: {df_traits.count():,}")

# Bước 2: Tính toán thống kê
total_traits = df_traits.count()

trait_stats = df_traits.groupBy("trait_id").agg(
    count("*").alias("count"),
    spark_round(avg("placement"), 2).alias("avg_place"),
    spark_round(avg("win") * 100, 1).alias("win_rate"),
    spark_round(avg("tier_current"), 1).alias("avg_tier"),
    spark_round(avg("num_units"), 1).alias("avg_units")
).withColumn(
    "frequency_pct", 
    spark_round((col("count") / total_traits) * 100, 2)
)

# Bước 3: Phân tích theo Tier Level
trait_tier_stats = df_traits.groupBy("trait_id", "tier_current").agg(
    count("*").alias("count"),
    spark_round(avg("placement"), 2).alias("avg_place"),
    spark_round(avg("win") * 100, 1).alias("win_rate")
)

window_spec = Window.partitionBy("trait_id").orderBy(desc("count"))

popular_tiers = trait_tier_stats.withColumn(
    "rank",
    row_number().over(window_spec)
).filter(
    col("rank") <= 3
).groupBy("trait_id").agg(
    collect_list(
        concat(lit("T"), col("tier_current").cast("string"))
    ).alias("popular_tiers")
)

# Bước 4: Tính Baseline và Place Change
overall_avg_placement = df.select(
    explode(col("info.participants")).alias("player")
).select(
    col("player.placement")
).agg(
    spark_round(avg("placement"), 2).alias("overall_avg")
).first()["overall_avg"]

print(f"Placement trung bình tổng thể: {overall_avg_placement}")

trait_place_change = df_traits.groupBy("trait_id").agg(
    spark_round(avg("placement"), 2).alias("avg_place_with_trait"),
    count("*").alias("count")
).withColumn(
    "place_change",
    spark_round(col("avg_place_with_trait") - lit(overall_avg_placement), 2)
)

# Bước 5: Join tất cả dữ liệu
final_result = trait_stats.join(popular_tiers, "trait_id", "left") \
                          .join(trait_place_change.select("trait_id", "place_change"), "trait_id", "left") \
                          .orderBy("place_change")

# Bước 6: Lọc traits phổ biến
min_count = 100
popular_traits = final_result.filter(col("count") >= min_count)
popular_traits_sorted = popular_traits.orderBy("place_change")

# Bước 7: Tạo bảng hình ảnh
pdf = popular_traits_sorted.limit(20).toPandas()

# Làm ngắn tên trait
pdf['Trait'] = pdf['trait_id'].str.replace('TFT15_', '').str.replace('tft15_', '')

# Lấy popular tiers (số units cần để kích hoạt)
pdf['Levels'] = pdf['popular_tiers'].apply(lambda x: ', '.join(x[:3]) if x else 'N/A')

# Tạo cột Frequency với format "count pct%"
pdf['Frequency'] = pdf.apply(lambda row: f"{row['count']:,} ({row['frequency_pct']}%)", axis=1)

# Tạo DataFrame hiển thị: Trait, Avg Place, Win Rate, Levels, Frequency
display_df = pdf[['Trait', 'avg_place', 'win_rate', 'Levels', 'Frequency']].copy()
display_df.columns = ['Trait', 'Avg Place', 'Win Rate', 'Levels', 'Frequency']

# Tạo bảng matplotlib
def create_table_image(df, filename='tft_traits_tier_list.png'):
    # Thêm cột rank
    df_display = df.copy()
    df_display.insert(0, '#', range(1, len(df_display) + 1))
    
    # Format Win Rate với %
    df_display['Win Rate'] = df_display['Win Rate'].apply(lambda x: f"{x}%")
    
    # Tạo figure với kích thước phù hợp
    fig, ax = plt.subplots(figsize=(14, len(df_display) * 0.4 + 2))
    ax.axis('tight')
    ax.axis('off')
    
    # Tạo bảng - 6 cột: #, Trait, Avg Place, Win Rate, Levels, Frequency
    table = ax.table(cellText=df_display.values,
                     colLabels=df_display.columns,
                     cellLoc='left',
                     loc='center',
                     colWidths=[0.05, 0.20, 0.12, 0.12, 0.18, 0.20])
    
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 2)
    
    # Style cho header
    for i in range(len(df_display.columns)):
        cell = table[(0, i)]
        cell.set_facecolor('#667eea')
        cell.set_text_props(weight='bold', color='white', fontsize=11)
    
    # Style cho các dòng
    for i in range(1, len(df_display) + 1):
        for j in range(len(df_display.columns)):
            cell = table[(i, j)]
            
            # Màu nền xen kẽ
            if i % 2 == 0:
                cell.set_facecolor('#f8f9fa')
            else:
                cell.set_facecolor('#ffffff')
            
            # Bold cho Avg Place và Win Rate
            if j in [2, 3]:  # Avg Place, Win Rate
                cell.set_text_props(weight='bold')
            
            # Rank column
            if j == 0:
                cell.set_facecolor('#667eea')
                cell.set_text_props(weight='bold', color='white')
    
    # Thêm title
    plt.title('TFT Traits Tier List - Set 15',
              fontsize=14, weight='bold', pad=20)
    
    # Lưu file
    plt.tight_layout()
    plt.savefig(filename, dpi=150, bbox_inches='tight', facecolor='white')
    print(f"\n✅ Đã lưu bảng vào file: {filename}")
    plt.close()
    
    return filename

# Hiển thị kết quả
print("=" * 100)
print(f"📊 TFT TRAITS TIER LIST - SET 15 (Sorted by Trait Impact)")
print("=" * 100)
print(f"📈 Tổng số traits phân tích: {popular_traits_sorted.count()}")
print(f"📊 Hiển thị Top 20 traits có impact tốt nhất")
print("=" * 100)
print("=" * 100)

# Tạo và lưu bảng
output_file = create_table_image(display_df, 'tft_traits_tier_list.png')

# Thống kê theo tier
from pyspark.sql.functions import count as spark_count_agg

print("\n📊 THỐNG KÊ THEO TIER:")
tier_stats = popular_traits.withColumn(
    "Tier",
    when(col("avg_place") < 3.80, "S")
    .when((col("avg_place") >= 3.80) & (col("avg_place") < 4.00), "A")
    .when((col("avg_place") >= 4.00) & (col("avg_place") < 4.20), "B")
    .when((col("avg_place") >= 4.20) & (col("avg_place") < 4.40), "C")
    .otherwise("D")
).groupBy("Tier").agg(
    spark_count_agg("*").alias("Count"),
    spark_round(avg("win_rate"), 1).alias("Avg Win Rate"),
    spark_round(avg("avg_place"), 2).alias("Avg Place")
).orderBy("Tier")

tier_stats.show()

# Dừng Spark
spark.stop()
