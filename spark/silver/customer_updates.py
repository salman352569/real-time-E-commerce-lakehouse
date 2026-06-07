from pyspark.sql.functions import lit ,current_timestamp,col
from spark.utils.spark_session import create_spark_session
from spark.utils.file_reader import read_csv
from spark.utils.logger import get_logger
import shutil 
import os 
logger = get_logger(__name__)

spark=create_spark_session("SCD2customerupdates")


try:
    logger.info("Strating SCD type processing")

    #Existing Dimensions
    dimension_df= spark.read.parquet("data/silver/customer_dimension")
    dimension_df = dimension_df.cache()
    dimension_df.count()

    current_df =dimension_df.filter(col("is_current") == True)

    #New update customer 

    updates_df =read_csv(spark,"data/raw/customer_update.csv")

    logger.info(f"Updates Received : {updates_df.count()} records")

    # Find Changed Customers
    changed_df = (
        current_df.alias("old")
        .join(
            updates_df.alias("new"),
            "customer_id"
        )
        .filter(
            (col("old.customer_city")
             != col("new.customer_city"))
             |
             (col("old.customer_state")
              != col("new.customer_state"))
        )
        
    )
    changed_count = changed_df.count()
    logger.info(f"Changed Customers : {changed_count}")

    if changed_count == 0 :
        logger.info("No customer changes found ")
        
        spark.stop()
        exit()

     # Expired Old records
    
    expired_records = (
        changed_df
        .select(
            "old.*"
        )
        .withColumn(
            "is_current",
            lit(False)
        )
        .withColumn(
            "end_date",
            current_timestamp()
        )
    )

    # New Records 
    new_records = (
        changed_df
        .select(
            col("new.customer_id").alias("customer_id"),
            col("new.customer_unique_id").alias("customer_unique_id"),
            col("new.customer_zip_code_prefix").alias("customer_zip_code_prefix"),
            col("new.customer_city").alias("customer_city"),
            col("new.customer_state").alias("customer_state"),
        )
        .withColumn(
            "effective_date",
            current_timestamp()
      )
      .withColumn(
          "end_date",
          lit(None)
      )
      .withColumn(
          "is_current",
          lit(True)
      
      )
    ) 

    #unchanged Records 
    changed_ids = changed_df.select("customer_id").distinct()

    unchanged_records = (
        dimension_df
        .join(
            changed_ids,
            "customer_id",
            "left_anti"
        )
    )
    # Final Dimensions 
    final_dimension = (
        unchanged_records
        .unionByName(
            expired_records
        )
        .unionByName(
            new_records
        )
        )
    logger.info(
    f"Current Dimension Records: {dimension_df.count()}"
    )

    logger.info(
        f"Changed Records: {changed_count}"
    )

    logger.info(
        f"Expired Records: {expired_records.count()}"
    )

    logger.info(
        f"New Records: {new_records.count()}"
    )

    logger.info(
        f"Final Dimension Records: {final_dimension.count()}"
    )
        

    temp_path = "data/silver/customer_dimension_temp"

    final_dimension.write.mode("overwrite").parquet(temp_path)
    if os.path.exists("data/silver/customer_dimension"):
       shutil.rmtree("data/silver/customer_dimension")
    os.rename(temp_path,"data/silver/customer_dimension")
    logger.info(
        "Customer dimension replaced successfully"
    )
    logger.info("SCD type 2 completed successfully")

    logger.info(
        f"Expired Records: "
        f"{expired_records.count()}"
    )
    logger.info(
        f"Inserted Records :"
        f"{new_records.count()}"
    )

except Exception as e :
    logger.error(f"SCD Type 2 Failed:{str(e)} ")
    raise 

finally:
    spark.stop()
    