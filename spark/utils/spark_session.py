from pyspark.sql import SparkSession

def create_spark_session(app_name):

    spark = SparkSession.builder \
         .appName(app_name) \
         .config(
             "spark.sql.shuffle.partitions",
             "8"
         ) \
         .config(
             "spark.sql.adaptive.enabled",
             "true"
         ) \
         .config(
              "spark.sql.parquet.mergeSchema",
              "true"
         )\
         .getOrCreate()
    return spark
