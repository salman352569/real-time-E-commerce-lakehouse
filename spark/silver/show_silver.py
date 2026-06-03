from spark.utils.spark_session import create_spark_session

spark = create_spark_session("ShowSilver")

df = spark.read.parquet(
    "data/silver/orders"
)
df.printSchema()

df.show(5)