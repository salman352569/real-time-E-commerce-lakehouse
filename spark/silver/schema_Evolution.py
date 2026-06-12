from spark.utils.spark_session import create_spark_session
from spark.utils.logger import get_logger
from pyspark.sql.functions import lit ,col
from spark.utils.file_reader import read_csv


logger = get_logger(__name__)

spark = create_spark_session("ShowSilver")
try:
    logger.info(f"Strating schema Evoultion check")

    existing_df =spark.read.parquet("data/silver/orders")

    new_df = read_csv(spark,"data/raw/new_orders_Schema.csv")

    existing_col= set(existing_df.columns)

    new_col = set(new_df.columns)

    added_col= (new_col - existing_col)

    missing_col = existing_col - new_col

    logger.info(f"New column added:{added_col}")

    #Adding missing columns to missing data 

    for column in added_col:
        existing_df =(
            existing_df
            .withColumn(column,lit(None))
        )
    
    for column in missing_col:
        new_df = new_df.withColumn(
            column,lit(None)
        )

    # Merging Schema 
    new_df =new_df.withColumn("order_id",col("order_id").cast("string"))

    final_df=(
        existing_df
        .unionByName(
            new_df,
            allowMissingColumns=True
        )
    )
    final_df.printSchema()

    logger.info(f"Final Rows: {final_df.count()}")

    final_df.write\
        .mode("overwrite") \
        .parquet("data/silver/orders_schema_evolved")

    logger.info(f"Schema Evolution completed")

except Exception as e:
    logger.error(f"Schema Evolution Failed:{str(e)}")

    raise
finally:
    spark.stop()
