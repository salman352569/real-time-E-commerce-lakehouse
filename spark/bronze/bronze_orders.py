from spark.utils.logger import get_logger
from spark.utils.spark_session import create_spark_session
from spark.utils.file_writer import write_parquet

from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import input_file_name,lit



logger = get_logger(__name__)

try:

    spark = create_spark_session(
        "BronzeOrders"
    )

    df =spark.read.parquet(
            "data/raw/large_orders"
        )

    bronze_df = df.withColumn(
            "ingestion_timestamp",
            current_timestamp()
        ).withColumn(
            "source_file",
            input_file_name()
        
        ).withColumn(
            "record_source",
            lit("olist_orders")
        )
        
    write_parquet(
            bronze_df,
            "data/bronze/orders",
            "order_status"
        )

    logger.info(
            f"Bronze load completed. Rows ={bronze_df.count()}"
    )

except Exception as e :

    logger.error(
        f"Bronze ingestion Failed: {str(e)}"
    )
    raise 