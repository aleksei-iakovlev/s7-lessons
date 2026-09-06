from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import datetime
from pyspark.sql.window import Window
import sys


def main():

    date = sys.argv[1]
    days_count = int(sys.argv[2])
    events_base_path = sys.argv[3]
    analytics_base_path = sys.argv[4]
    output_base_path = sys.argv[5]

    spark = SparkSession.builder \
        .master("yarn") \
        .appName(f"calculate_connection_interests_{date}_d{days_count}") \
        .getOrCreate()

    def input_event_paths(date, depth):
        dt = datetime.datetime.strptime(date, '%Y-%m-%d')
        return [f"{events_base_path}/date={(dt - datetime.timedelta(days=x)).strftime('%Y-%m-%d')}" for x in
                range(depth)]

    def calculate_connection_interests(date, depth, spark):

        paths = input_event_paths(date, depth)

        messages = spark.read\
            .option("basePath", events_base_path)\
            .parquet(*paths)\
            .where("""
                    event_type = 'message'
                    and event.message_to is not null
                """)

        directs = (
            messages
            .select(
                F.least(
                    F.col("event.message_from"),
                    F.col("event.message_to")
                ).alias("user_id_s"),
                F.greatest(
                    F.col("event.message_from"),
                    F.col("event.message_to")
                ).alias("user_id_r")
            )
            .distinct()
        )

        user_interests = spark.read.parquet(f"{analytics_base_path}/user_interests_d{depth}")

        user_likes = user_interests.select("user_id", F.col("like_tag_top_1"))\
            .union(user_interests.select("user_id", F.col("like_tag_top_2")))\
            .union(user_interests.select("user_id", F.col("like_tag_top_3")))\
            .toDF("user_id", "tag")

        window = Window.partitionBy("user_id").orderBy(F.desc("tag_count"), F.desc("tag"))

        likes = directs.join(user_likes, directs.user_id_r == user_likes.user_id, "left")\
            .drop("user_id_r")\
            .drop("user_id")\
            .withColumnRenamed("user_id_s", "user_id")\
            .groupBy("user_id", "tag")\
            .agg(F.count("tag").alias("tag_count"))\
            .withColumn("rank", F.row_number().over(window))\
            .where(F.col("rank") <= 3)\
            .groupBy("user_id").pivot("rank", [1, 2, 3]).agg(F.first("tag"))\
            .toDF("user_id", "direct_like_tag_top_1", "direct_like_tag_top_2", "direct_like_tag_top_3")

        user_dislikes = user_interests.select("user_id", F.col("dislike_tag_top_1").alias("tag"))\
            .union(user_interests.select("user_id", F.col("dislike_tag_top_2").alias("tag")))\
            .union(user_interests.select("user_id", F.col("dislike_tag_top_3").alias("tag")))

        dislikes = directs.join(user_dislikes, directs.user_id_r == user_dislikes.user_id, "left")\
            .drop("user_id_r")\
            .drop("user_id")\
            .withColumnRenamed("user_id_s", "user_id")\
            .groupBy("user_id", "tag")\
            .agg(F.count("tag").alias("tag_count"))\
            .withColumn("rank", F.row_number().over(window))\
            .where(F.col("rank") <= 3)\
            .groupBy("user_id").pivot("rank", [1, 2, 3]).agg(F.first("tag"))\
            .toDF("user_id", "direct_dislike_tag_top_1", "direct_dislike_tag_top_2", "direct_dislike_tag_top_3")

        subscriptions = spark.read\
            .option("mergeSchema", "true")\
            .parquet(events_base_path)\
            .where("event_type='subscription'")\
            .where(F.col("date") <= datetime.datetime.strptime(date, '%Y-%m-%d').date())\
            .select(F.col("event.user").alias("user_id"),\
                F.col("event.subscription_channel").alias("channel_id"))

        tags_verified = spark.read.parquet(f"{analytics_base_path}/verified_tags_candidates_d{depth}/date={date}")

        posts = spark.read\
            .option("basePath", events_base_path)\
            .parquet(*paths)\
            .where("event_type='message' and event.message_channel_to is not null")\
            .join(subscriptions, (F.col("event.message_channel_to") == F.col("channel_id")))\
            .select(F.col("event.user").alias("user_id"), F.explode(F.col("event.tags").alias("tag")))\
            .join(F.broadcast(tags_verified), "tag", "inner")

        channel_tags = posts\
            .groupBy("user_id", "tag")\
            .agg(F.count("*").alias("tag_count"))\
            .withColumn("rank", F.row_number().over(window))\
            .where(F.col("rank") <= 3)\
            .groupBy("user_id").pivot("rank", [1, 2, 3]).agg(F.first("tag"))\
            .toDF("user_id", "sub_verified_tag_top_1", "sub_verified_tag_top_2", "sub_verified_tag_top_3")        

        connection_interests = likes.join(dislikes, "user_id", "full").join(channel_tags, "user_id", "full")
        connection_interests.write.mode("overwrite").parquet(f"{output_base_path}/connection_interests_d{depth}/date={date}")
    
        return None

    calculate_connection_interests(date, days_count, spark)


if __name__ == "__main__":
    main()
