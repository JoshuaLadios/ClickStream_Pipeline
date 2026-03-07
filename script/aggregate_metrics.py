from pyspark.sql import SparkSession
from logger_config import logger
from s3_utils import get_s3_client

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

# User Behavior
def user_behavior(valid_df):
    valid_df.orderBy("event_date", ascending=False).show(10)
    
# Session Summary
def session_behavior():
    print("Session Behavior")

# Product Daily Metrics
def product_daily_metrics():
    print("Product Daily Metrics")

# Daily Business Metrics
def daily_business_metrics():
    print("Daily Business Metrics")

user_behavior(valid_df)