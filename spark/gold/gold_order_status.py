from pyspark.sql.functions import *
from spark.utils.spark_session import create_spark_session
from spark.utils.logger import get_logger

logger = get_logger(__name__)

spark = create_spark_session("GoldOrderStatusSummary")

try:

    orders_df=spark.read.parquet("data/silver/orders")
    order_status_summary= (
        orders_df.groupBy("order_status")
        .agg(
            count("*").alias("Total_orders")
        )
    )

    order_status_summary.write \
      .mode("overwrite") \
      .parquet("data/gold/order_status_summary")
    
    logger.info(f"Order status summary completed ")
    order_status_summary.show()

except Exception as e:
    logger.error(str(e))
    raise

finally:
    spark.stop()

