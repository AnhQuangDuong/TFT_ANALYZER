from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, count, avg, desc, round as spark_round, 
    collect_list, concat, lit, row_number, sum as spark_sum, when
)
from pyspark.sql.window import Window
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.table import Table
import matplotlib.patches as mpatches

# Khởi tạo Spark Session
spark = (SparkSession.builder
    .appName("TFT-Items-Tier-List")
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
    col("player.units")
)

df_units = df_players.select(
    col("placement"),
    explode(col("units")).alias("unit")
)

df_items = df_units.select(
    col("placement"),
    col("unit.character_id").alias("unit_name"),
    explode(col("unit.itemNames")).alias("item_id")
).filter(
    (col("item_id").isNotNull()) & 
    (col("item_id") != "") &
    (col("item_id") != "TFT_Item_EmptyBag")
)

df_items = df_items.withColumn(
    "win", 
    (col("placement") <= 4).cast("int")
)

print(f"Tổng số items tìm thấy: {df_items.count():,}")

# Bước 2: Tính toán thống kê
total_items = df_items.count()

item_stats = df_items.groupBy("item_id").agg(
    count("*").alias("count"),
    spark_round(avg("placement"), 2).alias("avg_place"),
    spark_round(avg("win") * 100, 1).alias("win_rate")
).withColumn(
    "frequency_pct", 
    spark_round((col("count") / total_items) * 100, 2)
)

# Bước 3: Tìm Popular Units
unit_item_counts = df_items.groupBy("item_id", "unit_name").count()
window_spec = Window.partitionBy("item_id").orderBy(desc("count"))

top_units_per_item = unit_item_counts.withColumn(
    "rank", 
    row_number().over(window_spec)
).filter(
    col("rank") <= 5
).groupBy("item_id").agg(
    collect_list("unit_name").alias("popular_units")
)

# Bước 4: Tính Baseline và Place Change
all_units_placements = df_units.select(
    col("placement"),
    col("unit.character_id").alias("unit_name")
)

unit_baseline = all_units_placements.groupBy("unit_name").agg(
    spark_round(avg("placement"), 2).alias("baseline_place"),
    count("*").alias("total_appearances")
)

unit_item_placements = df_items.groupBy("item_id", "unit_name").agg(
    spark_round(avg("placement"), 2).alias("avg_place_with_item"),
    count("*").alias("count_with_item")
)

unit_item_impact = unit_item_placements.join(
    unit_baseline,
    "unit_name",
    "inner"
).withColumn(
    "place_change",
    spark_round(col("avg_place_with_item") - col("baseline_place"), 2)
)

item_place_change = unit_item_impact.groupBy("item_id").agg(
    spark_round(
        spark_sum(col("place_change") * col("count_with_item")) / spark_sum(col("count_with_item")), 
        2
    ).alias("avg_place_change")
)

# Bước 5: Join tất cả dữ liệu
final_result = item_stats.join(top_units_per_item, "item_id", "left") \
                          .join(item_place_change, "item_id", "left") \
                          .orderBy("avg_place_change")

# Bước 6: Lọc items phổ biến
min_count = 100
popular_items = final_result.filter(col("count") >= min_count)
popular_items_sorted = popular_items.orderBy("avg_place_change")

# Bước 7: Tạo bảng HTML đẹp
pdf = popular_items_sorted.limit(20).toPandas()

# Làm ngắn tên item
pdf['Item'] = pdf['item_id'].str.replace('TFT_Item_', '').str.replace('TFT15_Item_', '').str.replace('TFT4_Item_', '').str.replace('_Artifact', '').str.replace('Ornn', '').str.replace('TFT5_Item_', '').str.replace('Radiant', '').str.replace('Artifact_', '')

# Lấy 5 tướng phổ biến nhất
pdf['Popular Units'] = pdf['popular_units'].apply(lambda x: ', '.join([u.replace('TFT15_', '').replace('tft15_', '') for u in x[:5]]) if x else 'N/A')

# Tạo DataFrame hiển thị
display_df = pdf[['Item', 'avg_place', 'avg_place_change', 'win_rate', 'count', 'frequency_pct', 'Popular Units']].copy()
display_df.columns = ['Item', 'Avg Place', 'Place Change', 'Win Rate %', 'Count', 'Freq %', 'Popular Units']

# Rút ngắn tên tướng để bảng gọn hơn
display_df['Popular Units'] = display_df['Popular Units'].str[:50] + '...'

# Tạo bảng matplotlib
def create_table_image(df, filename='tft_items_tier_list.png'):
    # Thêm cột rank
    df_display = df.copy()
    df_display.insert(0, '#', range(1, len(df_display) + 1))
    
    # Tạo figure với kích thước phù hợp
    fig, ax = plt.subplots(figsize=(20, len(df_display) * 0.4 + 2))
    ax.axis('tight')
    ax.axis('off')
    
    # Tạo bảng
    table = ax.table(cellText=df_display.values,
                     colLabels=df_display.columns,
                     cellLoc='left',
                     loc='center',
                     colWidths=[0.04, 0.15, 0.08, 0.09, 0.09, 0.07, 0.06, 0.42])
    
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 2)
    
    # Style cho header
    for i in range(len(df_display.columns)):
        cell = table[(0, i)]
        cell.set_facecolor('#667eea')
        cell.set_text_props(weight='bold', color='white', fontsize=10)
    
    # Style cho các dòng
    for i in range(1, len(df_display) + 1):
        for j in range(len(df_display.columns)):
            cell = table[(i, j)]
            
            # Màu nền xen kẽ
            if i % 2 == 0:
                cell.set_facecolor('#f8f9fa')
            else:
                cell.set_facecolor('#ffffff')
            
            # Màu cho Place Change
            if j == 3:  # Cột Place Change
                value = df_display.iloc[i-1, j]
                if value < 0:
                    cell.set_text_props(color='#22c55e', weight='bold')
                elif value > 0:
                    cell.set_text_props(color='#ef4444', weight='bold')
            
            # Bold cho Avg Place và Win Rate
            if j in [2, 4]:  # Avg Place, Win Rate
                cell.set_text_props(weight='bold')
            
            # Rank column
            if j == 0:
                cell.set_facecolor('#667eea')
                cell.set_text_props(weight='bold', color='white')
    
    # Thêm title
    plt.title('TFT Items Tier List - Set 15 (Sorted by Item Impact)\n' + 
              'Place Change ÂM = Item giúp tướng tốt hơn | Place Change DƯƠNG = Item làm tướng kém hơn',
              fontsize=14, weight='bold', pad=20)
    
    # Lưu file
    plt.tight_layout()
    plt.savefig(filename, dpi=150, bbox_inches='tight', facecolor='white')
    print(f"\n✅ Đã lưu bảng vào file: {filename}")
    plt.close()
    
    return filename

# Hiển thị kết quả
print("=" * 100)
print(f"📊 TFT ITEMS TIER LIST - SET 15 (Sorted by Item Impact)")
print("=" * 100)
print(f"📈 Tổng số items phân tích: {popular_items_sorted.count()}")
print(f"📊 Hiển thị Top 20 items có impact tốt nhất")
print("=" * 100)
print("\n🎨 Chú thích Place Change:")
print("  🟢 Place Change ÂM (xanh) = Item giúp tướng placement TỐT HƠN so với khi không có item")
print("  🔴 Place Change DƯƠNG (đỏ) = Item làm tướng placement KÉM HƠN so với khi không có item")
print("  📝 Place Change = Avg Place (with item) - Baseline Place (without item)")
print("=" * 100)

# Tạo và lưu bảng
output_file = create_table_image(display_df, 'tft_items_tier_list.png')

# Thống kê theo tier
from pyspark.sql.functions import count as spark_count_agg

print("\n📊 THỐNG KÊ THEO TIER:")
tier_stats = popular_items.withColumn(
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
