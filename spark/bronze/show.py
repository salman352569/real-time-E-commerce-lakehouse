from spark.utils.spark_session import create_spark_session

spark = create_spark_session(
    "bronzeShow"
)

df =spark.read.parquet("data/bronze/orders")
df.show(5)