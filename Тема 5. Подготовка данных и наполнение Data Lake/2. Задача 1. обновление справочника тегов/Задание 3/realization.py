from datetime import timedelta, datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def main():

    spark = SparkSession.builder \
                    .master("local") \
                    .appName("HASHTAGS") \
                    .getOrCreate()

    def input_paths(date, depth):
        dt = datetime.strptime(date, '%Y-%m-%d')
        return [f"/user/s19290263/data/events/date={(dt-timedelta(days=x)).strftime('%Y-%m-%d')}/event_type=message" for x in range(depth)]

    paths = input_paths('2022-05-31', 84)
    messages = spark.read.parquet(*paths)
    all_tags = messages.where("event.message_channel_to is not null")\
        .selectExpr(["event.message_from as user", "explode(event.tags) as tag"])\
        .groupBy("tag").agg(F.expr("count(distinct user) as suggested_count"))\
        .where("suggested_count >= 100")

    verified_tags = spark.read.parquet("/user/master/data/snapshots/tags_verified/actual")
    candidates = all_tags.join(verified_tags, "tag", "left_anti")
    candidates.show()
    candidates.write.mode('overwrite').parquet('/user/username/data/analytics/candidates_d84_pyspark')


if __name__ == "__main__":
    main()
