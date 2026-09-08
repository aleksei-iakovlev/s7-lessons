from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import datetime
from pyspark.sql.window import Window
import sys
from pyspark.sql.types import DoubleType


spark = SparkSession.builder \
    .master("yarn") \
    .appName("geoMart") \
    .getOrCreate()

events = spark.read.parquet('/user/master/data/geo/events')
geo_raw = spark.read.csv('/user/s19290263/data/geo.csv', sep=';', header=True)

geo = geo_raw.withColumn("lt", F.regexp_replace(F.trim(F.col("lat")), ",", ".").cast(DoubleType()))\
    .withColumn("ln", F.regexp_replace(F.trim(F.col("lng")), ",", ".").cast(DoubleType()))\
    .drop('lat', 'lng')

messages = events.where('event_type = "message" and  event.message_to is not null')\
    .sample(withReplacement=False, fraction=0.001, seed=42)
messages = messages.withColumn("event_id", F.monotonically_increasing_id())
messages = messages.cache()

def calc_distance(lat1, lon1, lat2, lon2):
    lat1 = F.radians(lat1)
    lon1 = F.radians(lon1)
    lat2 = F.radians(lat2)
    lon2 = F.radians(lon2)

    return 2 * F.lit(6371.0) * F.asin(
        F.sqrt(
            F.pow(F.sin((lat1 - lat2) / F.lit(2)), 2)
            + F.cos(lat1)
            * F.cos(lat2)
            * F.pow(F.sin((lon1 - lon2) / F.lit(2)), 2)
        )
    )

def find_city(df):
    window = Window.partitionBy("event_id").orderBy(F.col('distance').asc_nulls_last())
    return df.crossJoin(F.broadcast(geo))\
        .withColumn('distance', calc_distance(F.col('lat'), F.col('lon'), F.col('lt'), F.col('ln')))\
        .withColumn("rn", F.row_number().over(window))\
        .filter(F.col("rn") == 1)\
        .drop('lat', 'lon', 'lt', 'ln', 'distance', 'rn')\
        .withColumnRenamed('id', 'city_id')\
        .withColumn('local_time', F.from_utc_timestamp(F.col('event.message_ts'), F.col('timezone')))


def find_act_city(df):
    window = Window.partitionBy('event.message_from').orderBy(F.col('date').desc())
    return df.withColumn('rn', F.row_number().over(window))\
        .filter(F.col('rn') == 1)\
        .select(F.col('event.message_from').alias('user_id'), F.col('city').alias('act_city'))

def find_home_city(df):
    window = Window.partitionBy('user_id').orderBy(F.col('days_count').desc())
    return df.groupBy(F.col('event.message_from').alias('user_id'), F.col('city').alias('home_city'))\
        .agg(F.countDistinct('date').alias('days_count'))\
        .withColumn('rn', F.row_number().over(window))\
        .filter(F.col('rn') == 1)\
        .select(F.col('user_id'), F.col('home_city'))

def travel_count(df):
    return df.groupBy(F.col('event.message_from').alias('user_id'))\
        .agg(F.countDistinct('city_id').alias('cities_count'))\
        .orderBy(F.desc(F.col('cities_count')))

def travel_array(df):
    pass

messages_localized = find_city(messages).cache()
messages_localized.show(100)
# act_city = find_act_city(df)
# home_city = find_home_city(df)
# cityMart = act_city.join(home_city, 'user_id', 'left')
# cityMart.show()