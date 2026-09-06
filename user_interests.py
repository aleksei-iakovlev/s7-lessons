import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import datetime
from pyspark.sql.window import Window


def main():

    date = sys.argv[1]
    days_count = int(sys.argv[2])
    events_base_path = sys.argv[3]  # "/user/s19290263/data/events"
    output_base_path = sys.argv[4]  # /user/s19290263/data/analytics

    spark = SparkSession.builder \
        .master("yarn") \
        .appName(f"calculate_user_interests_{date}_{days_count}") \
        .getOrCreate()

    def input_event_paths(date, depth):
        dt = datetime.datetime.strptime(date, '%Y-%m-%d')
        return [f"{events_base_path}/date={(dt - datetime.timedelta(days=x)).strftime('%Y-%m-%d')}" for x in
                range(depth)]

    def calculate_user_interests(date, depth, spark):

        reaction_paths = input_event_paths(date, depth)

        reactions = spark.read\
            .option("basePath", events_base_path)\
            .parquet(*reaction_paths)\
            .where("event_type='reaction'")

        all_message_tags = spark.read\
            .option("mergeSchema", "true")\
            .parquet(events_base_path)\
            .where("event_type='message'")\
            .select(F.col("event.message_id").alias("message_id"),
                    F.col("event.message_from").alias("user_id"),
                    F.explode(F.col("event.tags")).alias("tag")
            )

        messages = spark.read\
            .option("basePath", events_base_path)\
            .parquet(*reaction_paths)\
            .where("event_type='message'")

        user_tags_count = messages.distinct()\
            .selectExpr(["event.message_from as user_id", "explode(event.tags) as tag"])\
            .groupBy("user_id", "tag")\
            .agg(F.count("*").alias("tag_count"))

        window = Window.partitionBy("user_id").orderBy(F.desc("tag_count"), F.desc("tag"))

        top_3_tags = user_tags_count\
            .withColumn("rank", F.row_number().over(window))\
            .where(F.col("rank") <= 3)\
            .groupBy("user_id").pivot("rank", [1, 2, 3]).agg(F.first("tag"))\
            .toDF("user_id", "tag_top_1", "tag_top_2", "tag_top_3")

        reaction_tags = reactions\
            .select(F.col("event.reaction_from").alias("user_id"), 
                    F.col("event.message_id").alias("message_id"), 
                    F.col("event.reaction_type").alias("reaction_type")
                    ).join(all_message_tags.select("message_id", "tag"), "message_id")

        reaction_tops = reaction_tags\
            .groupBy("user_id", "tag", "reaction_type")\
            .agg(F.count("*").alias("tag_count"))\
            .withColumn("rank", F.row_number().over(Window.partitionBy("user_id", "reaction_type")\
            .orderBy(F.desc("tag_count"), F.desc("tag"))))\
            .where("rank <= 3")\
            .groupBy("user_id", "reaction_type")\
            .pivot("rank", [1, 2, 3])\
            .agg(F.first("tag"))

        like_tops = reaction_tops\
            .where("reaction_type = 'like'")\
            .drop("reaction_type")\
            .withColumnRenamed("1", "like_tag_top_1")\
            .withColumnRenamed("2", "like_tag_top_2")\
            .withColumnRenamed("3", "like_tag_top_3")

        dislike_tops = reaction_tops\
            .where("reaction_type = 'dislike'")\
            .drop("reaction_type")\
            .withColumnRenamed("1", "dislike_tag_top_1")\
            .withColumnRenamed("2", "dislike_tag_top_2")\
            .withColumnRenamed("3", "dislike_tag_top_3")

        semi_result = like_tops.join(dislike_tops, "user_id", "full_outer")
        result = top_3_tags.join(semi_result, "user_id", "full_outer")\
            .withColumn("date", F.lit(date))

        result.write.mode("overwrite").parquet(f"{output_base_path}/user_interests_d{depth}")

        return None

    calculate_user_interests(date, days_count, spark)


if __name__ == "__main__":
    main()
