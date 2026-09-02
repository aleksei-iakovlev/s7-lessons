    import pyspark
    from pyspark.sql import SparkSession
    from pyspark.sql.window import Window
    import pyspark.sql.functions as F
    
    spark = SparkSession.builder \
                        .master("yarn") \
                        .appName("Learning DataFrames") \
                        .getOrCreate()
    
    events = spark.read.json("/user/master/data/events/date=2022-05-01")
    
window = Window().partitionBy('event.message_from').orderBy('event.message_ts')

dfWithLag = events.withColumn("lag_7",F.lag("event.message_to", 7).over(window))

dfWithLag.select('event.message_from', 'lag_7') \
    .filter(dfWithLag.lag_7.isNotNull()) \
    .orderBy(F.desc(F.col('event.message_from'))) \
    .show(10, False)  
