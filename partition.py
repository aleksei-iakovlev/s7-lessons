import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext


def main():
        date = sys.argv[1]
        base_input_path = sys.argv[2]
        base_output_path = sys.argv[3]

        conf = SparkConf().setAppName(f"EventsPartitioningJob-{date}")
        sc = SparkContext(conf=conf)
        sql = SQLContext(sc)

        events = sql.read.json(f"{base_input_path}/date={date}")

        events.write.format('parquet') \
                .partitionBy('event_type') \
                .mode('overwrite') \
                .save(f'{base_output_path}/date={date}')


if __name__ == "__main__":
        main()

# 2022-05-31 /user/master/data/events /user/s19290263/data/events
