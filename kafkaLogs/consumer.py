from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import regexp_extract
from pyspark import SparkContext
from pyspark.sql.types import StringType
# Import the KafkaConsumer from confluent_kafka
from confluent_kafka import KafkaException, KafkaError
from confluent_kafka import Consumer, KafkaError

# Function to process the log lines RDD
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import year, month, dayofmonth, hour, minute, second

# Specify the Kafka broker and topic to consume from
brokers = 'localhost:9092'
topic = 'log_lines_topic'
def process_logs(rdd):
    if not rdd.isEmpty():
        # Convert RDD to DataFrame
        logs_df = spark.read.json(rdd)

        # Data wrangling steps
        host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
        ts_pattern = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
        method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
        status_pattern = r'\s(\d{3})\s'
        content_size_pattern = r'\s(\d+)$'

        logs_df = logs_df.withColumn('host', regexp_extract('value', host_pattern, 1))
        logs_df = logs_df.withColumn('timestamp', regexp_extract('value', ts_pattern, 1))
        logs_df = logs_df.withColumn('method', regexp_extract('value', method_uri_protocol_pattern, 1))
        logs_df = logs_df.withColumn('endpoint', regexp_extract('value', method_uri_protocol_pattern, 2))
        logs_df = logs_df.withColumn('protocol', regexp_extract('value', method_uri_protocol_pattern, 3))
        logs_df = logs_df.withColumn('status', regexp_extract('value', status_pattern, 1).cast('integer'))
        logs_df = logs_df.withColumn('content_size', regexp_extract('value', content_size_pattern, 1).cast('integer'))

        # Add columns for year, month, and date
        logs_df = logs_df.withColumn('year', year(logs_df['timestamp']))
        logs_df = logs_df.withColumn('month', month(logs_df['timestamp']))
        logs_df = logs_df.withColumn('date', dayofmonth(logs_df['timestamp']))

        # Add columns for hour, minute, and second
        logs_df = logs_df.withColumn('hour', hour(logs_df['timestamp']))
        logs_df = logs_df.withColumn('minute', minute(logs_df['timestamp']))
        logs_df = logs_df.withColumn('second', second(logs_df['timestamp']))

        # Print the transformed DataFrame
        logs_df.show(10, truncate=True)

        # Store the transformed DataFrame as Parquet files
        logs_df.write.parquet('logs.parquet')

        # Store the Parquet files in HDFS
        # writes the transformed DataFrame to the local filesystem as Parquet files with the name logs.parquet. 
        # These Parquet files are created in the same directory where the consumer.py script is executed.
        logs_df.write.parquet('hdfs://localhost:9000/logs.parquet')

        # Download the processed DataFrame as CSV
        logs_df.write.csv('processed_logs.csv')



def get_kafka_stream(consumer):
    num = 0
    while True:
        try:
            message = consumer.poll(1.0)  # Poll for a single message
            if not message: 
                print("no msg")
                continue
            if message.error():
                print("Consumer error: {}".format(message.error()))
                continue
            num += 1
            print("before",num)
            yield message.value().decode('utf-8')
            print("after",num)

        except KeyboardInterrupt:
            break


# Create a SparkSession
spark = SparkSession.builder.appName('KafkaConsumer').getOrCreate()

# Create a StreamingContext with a batch interval of 10 seconds
ssc = StreamingContext(spark.sparkContext, 10)

# Set up the Kafka consumer
conf = {
    'bootstrap.servers': brokers,  # Kafka broker address
    'group.id': 'python-consumer',
    'auto.offset.reset': 'earliest',
    'api.version.request': True
}
consumer = Consumer(conf)
print('Kafka Consumer has been initiated...')

# Determine the name of the topic that should be consumed and subscribe to it.
print('Available topics to consume: ', consumer.list_topics().topics)

# Subscribe to the Kafka topic
consumer.subscribe([topic])
print('Subscribed to topic: ', topic)

# Create a Kafka stream
kafka_stream = ssc.queueStream([consumer])
print('Kafka Stream created...')

# Process the Kafka stream
kafka_stream.foreachRDD(lambda rdd: process_logs(rdd))

# Convert the generator into an RDD
# kafka_rdd = spark.sparkContext.parallelize(get_kafka_stream(consumer))
# print('Converted the generator into an RDD...')

# # Create a Kafka stream
# kafka_stream = ssc.queueStream([kafka_rdd])
# # kafka_stream = ssc.queueStream([get_kafka_stream(consumer)])
# print('Kafka Stream created...')

# # Get the log lines from the Kafka stream
# # Consume and process the messages using the generator function
# lines = kafka_stream.flatMap(lambda x: x.split('\n'))

# # Process the messages further (example: print the messages)
# lines.pprint(num=10)  # Limit the output to 10 lines

# lines.foreachRDD(lambda rdd: process_logs(rdd))

# Start the streaming context
ssc.start()
ssc.awaitTermination()
