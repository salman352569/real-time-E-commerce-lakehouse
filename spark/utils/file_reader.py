def read_csv(spark,path):

    return spark.read \
        .option("header",True) \
        .option("inferSchema",True)\
        .csv(path)