from spark.utils.spark_session import create_spark_session
from pyspark.sql.window import Window
from pyspark.sql.functions import col
from pyspark.sql.functions import row_number
from spark.utils.watermark import get_watermark
from pyspark.sql.functions import max as spark_max
from spark.utils.watermark import update_watermark   



spark = create_spark_session(
    "SilverOrders",

)

df =spark.read.parquet(
    "data/bronze/orders")

# Read last processed timestamp
last_watermark = get_watermark()

# Incremental load 
incremental_df = df.filter(
    col("order_purchase_timestamp") > last_watermark
)

window_spec = Window.partitionBy(
        "order_id"
    ).orderBy(
        col("ingestion_timestamp").desc()
    )

dedup_df = incremental_df.withColumn(
        "row_num",
        row_number().over(window_spec)
    ).filter(
        col("row_num") == 1
    ).drop("row_num")
new_watermark = (
    dedup_df
    .agg(
        spark_max(
            "order_purchase_timestamp"
        )
    )
    .collect()[0][0]
)
print(f"Old Watermark: {last_watermark}")
print(f"New Watermark: {new_watermark}")
update_watermark(str(new_watermark))

if dedup_df.count() == 0:
    print("No New recordsa are found ")
    spark.stop()
    exit()

dedup_df.write \
  .mode("append") \
  .partitionBy(
      "order_status"
  ) \
  .parquet(
      "data/silver/orders"
  )
print(
    f"Silver rows: {dedup_df.count()}"
)