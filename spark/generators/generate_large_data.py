from pyspark.sql.functions import rand
from pyspark.sql.functions import when
from pyspark.sql.functions import lit
from pyspark.sql.functions import monotonically_increasing_id

from spark.utils.spark_session import create_spark_session
from spark.utils.file_reader import read_csv
spark = create_spark_session(
          "GenerateLargeDatset")

orders_df = read_csv(
    spark,
    "data/raw/olist_orders_dataset.csv"
)

large_df =orders_df.crossJoin(
    spark.range(100)
)

#Creating skew intenionally
large_df = large_df.withColumn(
    "customer_id",
    when(
        rand() < 0.4,
        lit("SKEW_CUSTOMER")
    ).otherwise(large_df.customer_id)
)

# now creating monotonically increasing id 
large_df = large_df.withColumn(
    "synthetic_id",
    monotonically_increasing_id()
)
# Creating duplicates intentionally
duplicate_df = large_df.sample(
    fraction = 0.10,
    seed =42
)

large_df = large_df.union(
    duplicate_df
)


# now writing my data into parquet table 
large_df.write.mode("overwrite").parquet(
    "data/raw/large_orders"
)

print(
    f"Generated {large_df.count()} rows "
)