from logger_config import logger
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, FloatType, TimestampType, StructField, StructType
from pyspark.sql.functions import col, to_date, date_format

spark = SparkSession.builder \
    .appName("clickstream-pipeline").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Configure LocalStack S3
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://localstack:4566")
hadoop_conf.set("fs.s3a.access.key", "test")
hadoop_conf.set("fs.s3a.secret.key", "test")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("session_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("page_url", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("price", FloatType(), False),
    StructField("quantity", FloatType(), True),
    StructField("event_timestamp", StringType(), False)
])

df = spark.read.schema(schema).json("s3a://clickstream-datalake/events_02-19-26.json")
df = (
    df.filter(
        col("event_id").isNotNull() &
        col("user_id").isNotNull() &
        col("session_id").isNotNull() &
        col("event_type").isNotNull() &
        col("page_url").isNotNull() &
        col("product_id").isNotNull() &
        col("price").isNotNull() &
        col("event_timestamp").isNotNull()
        )  
        .withColumn("quantity", col("quantity").cast("int"))
        .na.fill({"quantity": 0})
        .withColumn("event_date", to_date("event_timestamp"))
        .withColumn("event_time", date_format("event_timestamp", "HH:mm:ss"))
)
df.show(5)

# from s3_utils import get_s3_client
# if __name__ == "__main__":
#     s3 = get_s3_client()
#     clickstream(s3)