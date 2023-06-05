import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, year, month, dayofmonth, hour, minute, second

# Specify the Kafka broker and topic to consume from
brokers = 'localhost:9092'
topic = 'log_lines_topic'

# Create a SparkSession
spark = SparkSession.builder.appName('KafkaConsumer').master("local[*]").getOrCreate()

# Define a foreachBatch function to process each micro-batch of the streaming DataFrame
def process_batch(df):
    # Convert the DataFrame to Pandas for easier manipulation
    pandas_df = df.toPandas()

    # Extract key and value columns as lists
    keys = pandas_df['key'].tolist()
    values = pandas_df['value'].tolist()

    # Process the key and value as per your requirement
    for key, value in zip(keys, values):
        # Log the key and value
        logging.info("Key: %s", key)
        logging.info("Value: %s", value)
        logging.info("------------------------")
        if key == "Stop":
            logging.info("Exiting consumer.....")
            query.stop()
            break

    # Data wrangling steps
    host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
    ts_pattern = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
    method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
    status_pattern = r'\s(\d{3})\s'
    content_size_pattern = r'\s(\d+)$'

    # Apply regex patterns to extract log fields
    df = df.withColumn('host', regexp_extract('value', host_pattern, 1))
    df = df.withColumn('timestamp', regexp_extract('value', ts_pattern, 1))
    df = df.withColumn('method', regexp_extract('value', method_uri_protocol_pattern, 1))
    df = df.withColumn('endpoint', regexp_extract('value', method_uri_protocol_pattern, 2))
    df = df.withColumn('protocol', regexp_extract('value', method_uri_protocol_pattern, 3))
    df = df.withColumn('status', regexp_extract('value', status_pattern, 1).cast('integer'))
    df = df.withColumn('content_size', regexp_extract('value', content_size_pattern, 1).cast('integer'))

    # Add columns for year, month, and date
    df = df.withColumn('year', year(df['timestamp']))
    df = df.withColumn('month', month(df['timestamp']))
    df = df.withColumn('date', dayofmonth(df['timestamp']))

    # Add columns for hour, minute, and second
    df = df.withColumn('hour', hour(df['timestamp']))
    df = df.withColumn('minute', minute(df['timestamp']))
    df = df.withColumn('second', second(df['timestamp']))

    # Print the transformed DataFrame
    df.show(10, truncate=True)

    # Store the transformed DataFrame as Parquet files
    df.write.mode("append").parquet('/tmp/output/logs.parquet')


# Configure logger for console output
logging.basicConfig(level=logging.INFO)

# Read the streaming DataFrame from Kafka
df = spark.readStream.format('kafka').option('kafka.bootstrap.servers', brokers).option('subscribe', topic).option('startingOffsets', 'latest').load()

# Convert key and value columns to string
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Apply the foreachBatch function to the streaming DataFrame
query = df.writeStream.foreachBatch(process_batch).start()

# Wait for the termination of the query
query.awaitTermination()
