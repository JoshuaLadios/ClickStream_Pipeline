from logger_config import logger
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, FloatType, StructField, StructType, DecimalType
from pyspark.sql.functions import col, to_date, date_format, to_timestamp, lit, when, current_timestamp
from s3_utils import get_s3_client

spark = SparkSession.builder.appName("clickstream-pipeline").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Configure LocalStack S3
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://localstack:4566")
hadoop_conf.set("fs.s3a.access.key", "test")
hadoop_conf.set("fs.s3a.secret.key", "test")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

s3 = get_s3_client()
Bucket_name = "clickstream-datalake"

schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("session_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("page_url", StringType(), True),
    StructField("product_id", StringType(), False),
    StructField("price", DecimalType(10, 2), True),
    StructField("quantity", FloatType(), True),
    StructField("event_timestamp", StringType(), False)
])

ALLOWED_EVENT_TYPE = {"purchase", "product_view", "add_to_cart"}
valid_df_map = ['event_id', 'user_id', 'session_id', 'event_type', 'page_url', 'product_id', 'price', 'quantity', 'revenue', 'event_date', 'event_time', 'ingestion_time']
invalid_df_map = ['event_id', 'user_id', 'session_id', 'event_type', 'page_url', 'product_id', 'price', 'quantity', 'event_date', 'event_time', 'ingestion_time', 'invalid_reasons']
initial_read = spark.read.schema(schema).json("s3a://clickstream-datalake/*.json")

def empty_df(spark, schema):
    return spark.createDataFrame([], schema)

def clean_and_validate(initial_read):
    try:
        df = initial_read.toDF(*[c.lower() for c in initial_read.columns])
        df = df.dropDuplicates(["event_id"])
        df = df.withColumn("quantity", col("quantity").cast("int"))
        df = df.na.fill({"quantity": 0, "price": 0.0})
        df = df.withColumn("event_timestamp", to_timestamp("event_timestamp", "yyyy-MM-dd'T'HH:mm:ss"))
        df = df.withColumn("event_date", to_date("event_timestamp"))
        df = df.withColumn("event_time", date_format("event_timestamp", "HH:mm:ss"))
        df = df.withColumn("ingestion_time", current_timestamp())
    except Exception as e:
        logger.error(f"Error in transformation: {e}")
        return empty_df(spark, initial_read.schema), empty_df(spark, initial_read.schema)
    
    try:
        valid_condition = (
                col("event_type").isin(ALLOWED_EVENT_TYPE) & 
                (
                    ((col("event_type") == "product_view") & (col("quantity") == 0)) |
                    ((col("event_type").isin(["add_to_cart", "purchase"])) & (col("quantity") > 0))
                ) &
                col("page_url").isNotNull() &
                col("quantity").isNotNull() &
                col("event_timestamp").isNotNull()
        )

        valid_df = df.filter(valid_condition)
        invalid_df = df.filter(~valid_condition)
        valid_df = valid_df.withColumn("revenue", col("quantity") * col("price"))

        invalid_df = invalid_df.withColumn(
            "invalid_reasons",
            when(~col("event_type").isin(ALLOWED_EVENT_TYPE), lit("invalid_event_type"))
            .when((col("event_type") == "product_view") & (col("quantity") != 0), lit("invalid_quantity"))
            .when(col("event_type").isin(["purchase", "add_to_cart"]) & (col("quantity") <= 0), lit("invalid_quantity"))
            .otherwise(lit("Others"))
        )
        
        valid_df = valid_df[valid_df_map]
        invalid_df = invalid_df[invalid_df_map]
        # valid_count = valid_df.count()
        # invalid_count = invalid_df.count()
        # logger.info(f"Valid data count: {valid_count} | Invalid data count: {invalid_count}")
        
    except Exception as e:
        logger.error(f"Failed in validation: {e}")
        return empty_df(spark, initial_read.schema), empty_df(spark, initial_read.schema)
    

    # Valid Data
    valid_df.write.mode("append").partitionBy("event_date").parquet("s3a://clickstream-datalake/silver/valid")

    # Invalid Data
    # invalid_df.write \
    # .mode("append") \
    # .partitionBy("event_date") \
    # .parquet("s3a://clickstream-datalake/silver/invalid")
    return valid_df, invalid_df