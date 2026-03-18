from pyspark.sql import SparkSession
from logger_config import logger
from s3_utils import get_s3_client
from pyspark.sql.functions import max as spark_max, min as spark_min
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("gold-layer").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Configure LocalStack S3
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://localstack:4566")
hadoop_conf.set("fs.s3a.access.key", "test")
hadoop_conf.set("fs.s3a.secret.key", "test")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

s3 = get_s3_client()
bucket_name = 'clickstream-datalake'
valid_df = spark.read.parquet("s3a://clickstream-datalake/silver/valid")

# User/Session Behavior
def user_session_behavior(valid_df):
    user_behavior_map = ['user_id', 'session_id', 'event_time', 'event_type', 'event_date']
    valid_df = valid_df[user_behavior_map]

    # Convert event_time to timestamp
    df = valid_df.withColumn("event_timestamp", F.to_timestamp(F.concat_ws(" ", "event_date", "event_time"), "yyyy-MM-dd HH:mm:ss")).cache()
    
    # Session Level-counts and counts per type
    events_count = (
        df.groupBy("user_id", "session_id", "event_type")
        .agg(F.count("*").alias("count"))
        .groupBy("user_id", "session_id")
        .agg(
            F.map_from_entries(F.sort_array(F.collect_list(F.struct("event_type", "count")))).alias("event_count_per_type")
        )
    )

    # Session Level Summary
    session_summary = (df.groupBy("user_id", "session_id").agg(
        F.min("event_date").alias("event_date"),
        F.count("*").alias("total_events"),
        F.min(F.struct("event_timestamp","event_type"))['event_type'].alias("first_event_type"),
        F.max(F.struct("event_timestamp","event_type"))['event_type'].alias("last_event_type"),
        F.countDistinct("event_type").alias("unique_event_types"),
        F.date_format(spark_min("event_timestamp"), "HH:mm:ss").alias("session_start_time"),
        F.date_format(spark_max("event_timestamp"), "HH:mm:ss").alias("session_end_time"),
        (spark_max("event_timestamp").cast("long") -  spark_min("event_timestamp").cast("long")).alias("session_duration_seconds")
    ))

    final_df = session_summary.join(events_count, on=["user_id", "session_id"], how="left")

    return final_df

# Purchases
purchase_df = valid_df.withColumnRenamed("event_date", "date")
purchase_df = purchase_df.filter(F.col("event_type") == "purchase").withColumn("purchase_revenue", F.col("price") * F.col("quantity")).cache()

# Product Daily Metrics
def product_daily_metrics(valid_df, purchase_df):
    daily_product_map = ['date', 'product_id', 'product_view', 'add_to_cart', 'purchase', 'revenue', 'unique_viewers', 'unique_buyers', 'conversion_rate']
    df = valid_df.withColumnRenamed("event_date", "date")

    purchase_agg = purchase_df.groupBy("date", "product_id").agg(
        F.round(F.sum("purchase_revenue"), 2).alias("revenue"),
        F.count("*").alias("purchase"),
        F.countDistinct("user_id").alias("unique_buyers")
    )

    event_agg = (df.groupBy("date", "product_id").agg(
        F.sum(F.when(F.col("event_type") == "product_view", 1).otherwise(0)).alias("product_view"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart"),
        # F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase"),
        # F.round(F.sum(F.when(F.col("event_type") == "purchase", F.col("price") * F.col("quantity")).otherwise(0)), 2).alias("revenue"),
        F.count_distinct(F.when(F.col("event_type") == "product_view", F.col("user_id"))).alias("unique_viewers"),
        # F.count_distinct(F.when(F.col("event_type") == "purchase", F.col("user_id"))).alias("unique_buyers")
        )
    )

    df_final = event_agg.join(purchase_agg, on=["date", "product_id"], how="left")

    df_final = df_final.withColumn(
        "conversion_rate", F.when(F.col("unique_viewers") == 0, 0).otherwise(F.round(F.coalesce(F.col("unique_buyers"), F.lit(0)) / F.col("unique_viewers") * 100, 2))
    )
    
    df_final = df_final[daily_product_map]
    print("Product Daily Metrics")
    return df_final

# Daily Business Metrics
def daily_business_metrics(valid_df, purchase_df):
    daily_business_map = ['date', 'total_revenue', 'total_orders', 'total_product_views', 'total_add_to_cart', 'unique_users', 'conversion_rate', 'avg_revenue_per_user', 'avg_orders_per_user']
    df = valid_df.withColumnRenamed("event_date", "date")

    purchase_agg = purchase_df.groupBy("date").agg(
        F.round(F.sum("purchase_revenue"), 2).alias("total_revenue"),
        F.count("*").alias("total_orders"),
        F.countDistinct("user_id").alias("unique_buyers")
    )

    event_agg = (df.groupBy("date").agg(
        # F.round(F.sum(F.when(F.col("event_type") == "purchase", F.col("price") * F.col("quantity")).otherwise(0)), 2).alias("total_revenue"),

        # Orders
        # F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("total_orders"),

        # Events
        F.sum(F.when(F.col("event_type") == "product_view", 1).otherwise(0)).alias("total_product_views"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("total_add_to_cart"),

        # Users
        F.countDistinct("user_id").alias("unique_users"),
        F.countDistinct(F.when(F.col("event_type") == "product_view", F.col("user_id"))).alias("unique_viewers"),
        # F.countDistinct(F.when(F.col("event_type") == "purchase", F.col("user_id"))).alias("unique_buyers"),
        )
    )

    df_final = event_agg.join(purchase_agg, on="date", how="left")

    df_final = df_final.withColumn(
        "conversion_rate", 
        F.when(F.col("unique_viewers") == 0, 0)
        .otherwise(F.round(F.coalesce(F.col("unique_buyers"), F.lit(0)) / F.col("unique_viewers") * 100, 2))
    ).withColumn(
        "avg_revenue_per_user", 
        F.round(F.col("total_revenue") / F.col("unique_users"), 2)
    ).withColumn(
        "avg_orders_per_user", 
        F.round(F.col("total_orders") / F.col("unique_users"), 2)
    )
    
    df_final = df_final[daily_business_map]
    print("Daily Business Metrics")
    return df_final

user_session_behavior_df = user_session_behavior(valid_df)
product_daily_metrics_df = product_daily_metrics(valid_df, purchase_df).show(5)
daily_business_metrics_df = daily_business_metrics(valid_df, purchase_df).show(5)


# List of all columns
# cols = valid_df.columns

# Create an aggregation dictionary
# agg_exprs = [F.count(c).alias(c + "_non_null") for c in cols]

# Aggregate
# column_counts = valid_df.agg(*agg_exprs)
# column_counts.show()
