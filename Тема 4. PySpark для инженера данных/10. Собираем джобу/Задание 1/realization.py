import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()
events = spark.read.json("/user/master/data/events/date=2022-05-31")

events.write.format('parquet') \
    .mode('overwrite') \
    .partitionBy('event_type'=='message')
    .save('/user/s19290263/data/events')

events.orderBy(F.col('event.datetime').desc()).show(10)
