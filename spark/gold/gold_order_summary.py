from pyspark.sql.functions import *
from spark.utils.spark_session import create_spark_session
from spark.utils.logger import get_logger

logger = get_logger(__name__)

spark = create_spark_session("GoldOrderSummary")

try:
    logger.info("Starting Gold layer Summary")

    orders_df= spark.read.parquet("data/silver/orders")

    daily_summary =(
        orders_df
        .withColumn("order_date",
        to_date(col("order_purchase_timestamp"
                    )
                  )
                )
        .groupBy("order_date")
        .agg(
            count("order_id")
            .alias("total_orders")
        )
    ) 
    daily_summary.write \
      .mode("overwrite") \
      .parquet("data/gold/dail_summary")
      
    logger.info(f"GOld Records : {daily_summary.count()}")

except Exception as e:
    logger.error(f"GOld layer Failed:{str(e)}")
    raise
finally:
    spark.stop()


