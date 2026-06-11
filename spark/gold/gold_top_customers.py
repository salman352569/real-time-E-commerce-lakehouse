from pyspark.sql.functions import *
from spark.utils.spark_session import create_spark_session
from spark.utils.logger import get_logger

logger = get_logger(__name__)

spark = create_spark_session("GoldTopcustomers")

try:

    orders_df = spark.read.parquet("data/silver/orders")

    top_customer_df =( 
        orders_df 
        .groupBy("customer_id")
        .agg(
            count("*").alias("total_orders")

        )
        .orderBy(desc("total_orders"))
        .limit(10)
    )


    top_customer_df.write.mode("overwrite").parquet("data/gold/top_customers")
    top_customer_df.show(truncate=False)

except Exception as e:
    logger.error(str(e))
    raise

finally:
    spark.stop()
    

                      