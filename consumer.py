from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, desc, count
from pyspark.sql.types import *
from datetime import datetime, timedelta

# Kafka consumer settings
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'log_lines_topic'
kafka_group_id = 'spark-streaming-consumer-group'

# Create a SparkSession
spark = SparkSession.builder.appName("KafkaConsumerSparkStreaming").getOrCreate()

# Define the Kafka source options
kafka_options = {
    "kafka.bootstrap.servers": kafka_bootstrap_servers,
    "subscribe": kafka_topic,
    "startingOffsets": "latest",
    "failOnDataLoss": "false"
}

# Read the Kafka stream using spark.readStream
df = spark.readStream.format("kafka").options(**kafka_options).load()

# Convert the binary message value to string
df = df.withColumn("value", col("value").cast("string"))

# Extract the desired fields from the log line
df = df.withColumn(
    "ip_address",
    regexp_extract(col("value"), r'^([\d.]+)', 1)
).withColumn(
    "timestamp",
    regexp_extract(col("value"), r'\[([^:]+)', 1)
).withColumn(
    "method",
    regexp_extract(col("value"), r'\"(\w+)', 1)
).withColumn(
    "endpoint",
    regexp_extract(col("value"), r'\"(?:[A-Z]+\s)?(\S+)', 1)
).withColumn(
    "http_version",
    regexp_extract(col("value"), r'HTTP/(\d+\.\d+)', 1)
).withColumn(
    "response_code",
    regexp_extract(col("value"), r'\s(\d{3})\s', 1)
).withColumn(
    "bytes",
    regexp_extract(col("value"), r'\s(\d+)$', 1)
)

# Select the desired columns
parsed_df = df.select("ip_address", "timestamp", "method", "endpoint", "http_version", "response_code", "bytes")

# Filter the DataFrame for response_code 404
filtered_df = parsed_df.filter(parsed_df.response_code == "404")

endpoints = filtered_df.groupBy("endpoint").agg(count("endpoint").alias("count"))

sorted_endpoints = endpoints.orderBy(desc("count"))

top_10_endpoints = sorted_endpoints.limit(10)

# Process the Kafka messages and write to console and text file
query = top_10_endpoints.writeStream.outputMode("complete").format("console").start()

# Process the Kafka messages and write to the text file
w_query = endpoints.writeStream.outputMode("complete").foreachBatch(
    lambda batch_df, batch_id: batch_df.coalesce(1).write.mode("append").option("header", "true").csv('/tmp/output/404')
).start()

# Set the current time as the start time
start_time = datetime.now()

while query.isActive:
    # Check if no messages have been received for 10 seconds
    elapsed_time = datetime.now() - start_time
    if elapsed_time > timedelta(seconds=100):
        query.stop()
    
    query.awaitTermination(1)  # Wait for 1 second

# Stop the SparkSession
spark.stop()
