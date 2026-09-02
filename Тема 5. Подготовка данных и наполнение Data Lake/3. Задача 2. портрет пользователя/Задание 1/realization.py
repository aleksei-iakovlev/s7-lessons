from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime, timedelta
from pyspark.sql.window import Window


spark = SparkSession.builder \
    .master("local") \
    .appName("tag_tops") \
    .getOrCreate()


def tag_tops(date, depth, spark):
    dt = datetime.strptime(date, '%Y-%m-%d')
    paths = [f"/user/s19290263/data/events/date={(dt-timedelta(days=x)).strftime('%Y-%m-%d')}/event_type=message" for x in range(depth)]
    messages = spark.read.parquet(*paths)
    
    user_tags_count = messages.distinct().selectExpr(["event.message_from as user_id", "explode(event.tags) as tag"])\
        .groupBy("user_id", "tag")\
        .agg(F.count("*").alias("tag_count")).show()

    window = Window.partitionBy("user_id").orderBy(F.desc("tag_count"), F.desc("tag"))

    top_3_tags = user_tags_count.withColumn("rank", F.row_number().over(window))\
        .where(F.col("rank") <= 3)\
        .groupBy("user_id").pivot("rank", [1, 2, 3]).agg(F.first("tag"))\
        .toDF("user_id", "tag_top_1", "tag_top_2", "tag_top_3")
    
    return top_3_tags

#tag_tops('2022-05-04', 5, spark).where(F.col("user_id")=="1009").show()

tag_tops('2022-06-04', 5, spark).repartition(1).write.mode('overwrite').parquet('/user/s19290263/data/tmp/tag_tops_06_04_5')
tag_tops('2022-05-04', 5, spark).repartition(1).write.mode('overwrite').parquet('/user/s19290263/data/tmp/tag_tops_05_04_5')
tag_tops('2022-05-04', 1, spark).repartition(1).write.mode('overwrite').parquet('/user/s19290263/data/tmp/tag_tops_05_04_1')
