from spark.utils.spark_session import create_spark_session
from pyspark.sql.window import Window
from pyspark.sql.functions import col,current_timestamp,when ,lit
from pyspark.sql.functions import row_number
from spark.utils.watermark import get_watermark
from pyspark.sql.functions import max as spark_max
from spark.utils.watermark import update_watermark 
from spark.utils.logger import get_logger  

logger= get_logger(__name__)

spark = create_spark_session(
    "SilverOrders",

)

df =spark.read.parquet(
    "data/bronze/orders")

df= df.withColumn(
    "arrival_timestamp",
    current_timestamp()
)

# Read last processed timestamp
last_watermark = get_watermark()


window_spec = Window.partitionBy(
        "order_id"
    ).orderBy(
        col("ingestion_timestamp").desc()
    )

dedup_df = df.withColumn(
        "row_num",
        row_number().over(window_spec)
    ).filter(
        col("row_num") == 1
    ).drop("row_num")

# Detect Late Data 

dedup_df=dedup_df.withColumn(
    "is_late",
    when(
        col("order_purchase_timestamp") < lit(last_watermark),
        True
    ).otherwise(False)
)
#Split Late order data and normal order data 
late_order_df=dedup_df.filter(col("is_late") == True)

normal_order_df =dedup_df.filter(col("is_late") == False)
total_count = dedup_df.count()
late_count = late_order_df.count()
normal_count = normal_order_df.count()
print(total_count)
print(late_count)
print(normal_count)


# Now writing late orders separately 
late_order_df.write.mode("overwrite").parquet("data/silver/late_orders")

#Now applying incremental logic to norrmal data 
incremental_df= normal_order_df.filter(
    col("order_purchase_timestamp") > last_watermark )
incremental_count= incremental_df.count()
print(incremental_count)

incremental_count = incremental_df.count()


if incremental_count > 0:
    
    new_watermark = (
            incremental_df
            .agg(
                spark_max(
                    "order_purchase_timestamp"
                )
            )
            .collect()[0][0]
    )
    update_watermark(str(new_watermark))

logger.info(f"Old Watermark: {last_watermark}")
logger.info(f"New Watermark: {new_watermark}")

new_watermark=last_watermark
if incremental_count == 0:
    print("No New incremental records found ")
    spark.stop()
    exit(0)
    

incremental_df.write \
  .mode("append") \
  .partitionBy(
      "order_status"
  ) \
  .parquet(
      "data/silver/orders"
  )
logger.info(
    f"Loaded silver cleaned  data : {dedup_df.count()}"
)