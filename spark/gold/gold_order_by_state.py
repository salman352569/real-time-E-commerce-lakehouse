from pyspark.sql.functions import count
from spark.utils.spark_session import create_spark_session
from spark.utils.logger import get_logger

logger = get_logger(__name__)

spark=create_spark_session("GoldOrderStateSummary")

try:

    logger.info(
        "Starting Order by State summary"
    )

    orders_df =spark.read.parquet("data/silver/orders")

    customer_dim =spark.read.parquet("data/silver/customer_dimension")

    current_customer = customer_dim.filter(customer_dim.is_current==True)
    

    state_summary = (
        orders_df.join(current_customer,"customer_id","inner")

    
                 .groupby("customer_state")
                 .agg(count("*").alias("total_orders")
                      )
    )

    state_summary.write.mode("overwrite").parquet("data/gold/order_state_summary")

    logger.info(f"states processed :{state_summary.count()}")

    state_summary.show(
        truncate=False
    )

except Exception as e:
    logger.error(str(e))
    raise

finally:
    spark.stop