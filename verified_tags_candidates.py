import sys
from datetime import timedelta, datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def main():

    date = sys.argv[1]
    depth = sys.argv[2]
    treshold = sys.argv[3]
    src = sys.argv[4]
    src_tags_v = sys.argv[5]
    dst = sys.argv[6]

    spark = SparkSession.builder \
                    .master("local") \
                    .appName(f"VerifiedTagsCandidatesJob-{date}-d{depth}-cut{treshold}") \
                    .getOrCreate()

    def input_paths(date, depth):
        dt = datetime.strptime(date, '%Y-%m-%d')
        return [f"{src}/date={(dt-timedelta(days=x)).strftime('%Y-%m-%d')}/event_type=message" for x in range(int(depth))]

    paths = input_paths(date, depth)
    messages = spark.read.parquet(*paths)
    all_tags = messages.where("event.message_channel_to is not null")\
        .selectExpr(["event.message_from as user", "explode(event.tags) as tag"])\
        .groupBy("tag").agg(F.expr("count(distinct user) as suggested_count"))\
        .where("suggested_count >= {treshold}")

    verified_tags = spark.read.parquet(src_tags_v)
    candidates = all_tags.join(verified_tags, "tag", "left_anti")
    candidates.show()
    candidates.write.mode('overwrite').parquet(f'{dst}/date={date}/verified_tags_candidates.py')


if __name__ == "__main__":
    main()
