from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("My second session") \
    .master("yarn") \
    .config("spark.driver.memory", "1g") \
    .config("spark.driver.cores", "2") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.yarn.am.memory", "2g") \
    .config("spark.yarn.am.cores", "2") \
    .getOrCreate()
