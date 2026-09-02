from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime, timedelta
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .master("yarn") \
    .appName("reactions_tops") \
    .getOrCreate()


def reaction_tag_tops(date, depth, spark):
    end_dt = datetime.strptime(date, '%Y-%m-%d')
    start = (end_dt - timedelta(days=depth - 1)).strftime('%Y-%m-%d')
    end = end_dt.strftime('%Y-%m-%d')

    events = spark.read.option("mergeSchema", "true").parquet("/user/s19290263/data/events")

    reactions = (events
             .filter((F.col("event_type") == "reaction") &
                     F.to_date(F.col("event.datetime")).between(F.lit(start), F.lit(end)))
             .select(F.col("event.message_id").alias("message_id"),
                     F.col("event.reaction_from").alias("user_id"),
                     F.col("event.reaction_type").alias("reaction_type"))
             .distinct())

    message_tags = (events
                .filter((F.col("event_type") == "message") &
                        (F.to_date(F.col("event.datetime")) <= F.lit(end)))
                .select(F.col("event.message_id").alias("message_id"),
                        F.explode("event.tags").alias("tag"))
                .distinct())

    ut = reactions.join(message_tags, "message_id", "inner")

    counts = (ut.groupBy("user_id", "tag")
              .agg(F.sum(F.when(F.col("reaction_type") == "like", 1).otherwise(0)).alias("likes"),
                   F.sum(F.when(F.col("reaction_type") == "dislike", 1).otherwise(0)).alias("dislikes")))

    def top3(count_col, prefix):
        w = Window.partitionBy("user_id").orderBy(F.desc(count_col), F.desc("tag"))
        return (counts
                .withColumn("rn", F.row_number().over(w))
                .filter(F.col("rn") <= 3)
                .groupBy("user_id")
                .pivot("rn", [1, 2, 3])
                .agg(F.first("tag"))
                .select("user_id",
                        *[F.col(str(i)).alias(f"{prefix}_tag_top_{i}") for i in (1, 2, 3)]))

    return top3("likes", "like").join(top3("dislikes", "dislike"), "user_id", "outer")


reaction_tag_tops('2022-05-04', 5, spark).write.mode('overwrite').parquet('/user/s19290263/data/tmp/reaction_tag_tops_05_04_5')
# reaction_tag_tops('2022-04-04', 5, spark).write.mode('overwrite').parquet('/user/s19290263/data/tmp/reaction_tag_tops_04_04_5')
# reaction_tag_tops('2022-04-04', 1, spark).write.mode('overwrite').parquet('/user/s19290263/data/tmp/reaction_tag_tops_04_04_1')
# reaction_tag_tops('2022-05-04', 5, spark).show()