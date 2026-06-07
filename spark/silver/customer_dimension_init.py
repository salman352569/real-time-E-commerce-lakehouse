from pyspark.sql.functions import current_timestamp,lit,col
from spark.utils.spark_session import create_spark_session
from spark.utils.file_reader import read_csv
from spark.utils.logger import get_logger

logger= get_logger(__name__)

spark=create_spark_session("SCD2customers")

try:

    customer_df=read_csv(spark,"data/raw/olist_customers_dataset.csv")

    dimension_path= (
        "data/silver/customer_dimension"
    )

    #Initial Load
    import os 
    if not os.path.exists(dimension_path):

        logger.info(
            "creating Initial customer Dimension"
        
        )
        dimension_df=(
            customer_df
            .withColumn("effective_date",
                        current_timestamp()
            )
            .withColumn("end_date",lit(None))
            .withColumn("is_current",lit(True))
            
            
            )
        dimension_df.write.mode("overwrite").parquet(dimension_path)

        logger.info(f"Initial Dimension Created : {dimension_df.count()} records")
    else:
        logger.info("Customer Dimension already exists")

except Exception as e:
    logger.error(f"SCD2 Failed : {str(e)}")   

    raise