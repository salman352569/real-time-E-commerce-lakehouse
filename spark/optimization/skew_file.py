# Here we are trying to solve Data skewness by using salting method 
from pyspark.sql.functions import floor,lit,rand,col,concat
from spark.utils.spark_session import create_spark_session
from spark.utils.logger import get_logger

logger = get_logger(__name__)

spark = create_spark_session("DataSkewness")

orders_df = spark.read.parquet("data/silver/orders")

# Adding Salt 

orders_df = orders_df.withColumn(
            "salt",
            floor(rand() * 10)
)

orders_df = orders_df.withColumn(
    "salted_customer",
    concat(
        col("customer_id"),
        lit("_"),
        col("salt")
    )
)
