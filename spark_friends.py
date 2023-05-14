import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

from computations import user_connections, heavyN, most_popular_photos


users_schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("country", StringType(), False),
    ])


events_schema = StructType([
        StructField("source", StringType(), False),
        StructField("target", StringType(), False),
        StructField("event", StructType([
            StructField("name", StringType(), False),
            StructField("weight", IntegerType(), False),
            StructField("event_date", DateType(), False),
        ]), False),
    ])


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Spark Friends") \
        .enableHiveSupport() \
        .getOrCreate()


    # users_df = spark.read.schema(users_schema).json("users.json").write.mode("overwrite").saveAsTable("users")
    # users_df.show(truncate=False)
    # events_df = spark.read.schema(events_schema).json("events.json").write.mode("overwrite").saveAsTable("events")
    # events_df.show(truncate=False)

    # Hive queries
    # spark.sql("show databases").show(truncate=False)
    # spark.sql("show tables in default").show(truncate=False)


    # 1. list connections for each of the users
    user_connections(spark)

    # 2. show top N most heavy connections
    if len(sys.argv) < 2:
        print("ERROR: Missing argument much?")
    else:
        n = int(sys.argv[1])
        heavyN(spark, n)

    # 3. show user whose photos are the most popular
    most_popular_photos(spark)

    # 4. use hive script to show these results with user's names
    
    spark.stop()