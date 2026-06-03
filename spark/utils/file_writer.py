def write_parquet(df, path, partition_col=None):

    writer = df.write.mode("append")

    if partition_col:
        writer = writer.partitionBy(
            partition_col
        )

    writer.parquet(path)