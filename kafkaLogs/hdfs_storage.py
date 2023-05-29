from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName('ParquetFileHDFS').getOrCreate()

# Read the Parquet file into a DataFrame
logs_df = spark.read.parquet('logs.parquet')

# Store the Parquet files in HDFS
logs_df.write.parquet('hdfs://localhost:9000/logs.parquet')
