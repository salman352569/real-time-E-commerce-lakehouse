from spark.utils.spark_session import create_spark_session
from spark.utils.file_reader import read_csv
from pyspark.sql.functions import col


spark = create_spark_session("ShowSilver")

existing_df = spark.read.parquet(
    "data/silver/orders"
)
existing_df.printSchema()

new_df =read_csv(spark,"data/raw/new_orders_Schema.csv")

new_df= new_df.withColumn("order_id",col("order_id").cast("int"))
