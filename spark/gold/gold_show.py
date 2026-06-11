from spark.utils.spark_session import create_spark_session


spark=create_spark_session("GoldShow")
df=spark.read.parquet("data/gold/dail_summary")
df.show(5)
    