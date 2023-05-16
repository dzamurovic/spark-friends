from pyspark.sql.functions import first, lit, col
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, sum as _sum


def user_connections(spark):
    events_df = spark.table("events")

    out_window_spec  = Window.partitionBy("source", "target").orderBy("source")
    outgoing_connections = events_df.select("source", "target") \
        .withColumn("row_number", row_number().over(out_window_spec)) \
        .where(col("row_number") == 1) \
        .drop("row_number") \
        .withColumnRenamed("source", "user_id")

    in_window_spec  = Window.partitionBy("target", "source").orderBy("target")
    incoming_connections = events_df.select("target", "source") \
        .withColumn("row_number", row_number().over(in_window_spec)) \
        .where(col("row_number") == 1) \
        .drop("row_number") \
        .withColumnRenamed("target", "user_id") \
        .withColumnRenamed("source", "target")

    outgoing_connections.union(incoming_connections) \
        .dropDuplicates() \
        .orderBy("user_id", "target") \
        .write.mode("overwrite") \
        .saveAsTable("user_connections")


def heavyN(spark, n=5):
    # Connection weight is not symmetrical.
    # Someone could be stalking somebody else while the other party
    # might not know the stalker exists.
    user_connections = spark.table("user_connections")
    events = spark.table("events")

    with_weight = user_connections.join(events, (user_connections.user_id == events.source) & (user_connections.target == events.target)) \
        .select(user_connections.user_id, user_connections.target, events.event.weight.alias("weight")) \
        .groupBy("user_id", "target").agg(_sum("weight").alias("weight")) \
        .orderBy("weight", ascending=False) \
        .limit(n) \
        .write.mode("overwrite") \
        .saveAsTable("heavyN")


def most_popular_photos(spark):
    events = spark.table("events")

    photo_events = events.where(events.event["name"].like("photo%"))
    photo_events.select(photo_events.target.alias("user_id"), photo_events.event["weight"].alias("weight")) \
        .groupBy("user_id").agg(_sum("weight").alias("weight")) \
        .orderBy("weight", ascending=False) \
        .write.mode("overwrite") \
        .saveAsTable("most_popular_photos")





