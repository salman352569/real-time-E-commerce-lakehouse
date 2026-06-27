from spark.utils.spark_session import create_spark_session

spark = create_spark_session(
    "bronzeShow"
)
df_bronze_order=spark.read.parquet("data/bronze/orders")
print(f"bronze:{df_bronze_order.count()}")


df_generate =spark.read.parquet("data/raw/large_orders")
print(f"large data: {df_generate.count()}")

df_silver =spark.read.parquet("data/silver/orders")
print(f"silver data:{df_silver.count()}")
