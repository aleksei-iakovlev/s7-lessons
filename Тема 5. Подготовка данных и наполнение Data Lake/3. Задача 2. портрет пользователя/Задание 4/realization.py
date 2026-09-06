from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .master("local") \
        .appName("compare_df") \
        .getOrCreate()

def compare_df(left, right):
    return (set(left.columns) == set(right.columns)) & \
        (left.count() == right.count() == left.union(right).distinct().count())


left = spark.read.parquet('/user/examples/interests_d28')
right = spark.read.parquet('/user/s19290263/analytics/user_interests_d28')

print(compare_df(left, right))
