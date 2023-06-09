from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode
from pyspark.sql.functions import col, lower, desc
from pyspark.conf import SparkConf
import psutil

#*********************************************************************#
#                      Funtion to log memory profile
#*********************************************************************#
def get_memory_usage():
    memory_info = psutil.virtual_memory()
    return memory_info.used

#*********************************************************************#
#                      Setting up spark 
#*********************************************************************#
conf = SparkConf()
conf.set("spark.driver.log.level", "INFO")
conf.set("spark.executor.log.level", "WARN")

# Create a SparkSession
spark = SparkSession.builder.appName("WordCount").config(conf=conf).getOrCreate()

# Read the input text file into a DataFrame
input_file = "/Users/rushshah/SCU/BigData/data_16GB.txt"
df = spark.read.text(input_file)

# Split each line into words and explode the resulting array
words_df = df.select(explode(split(df.value, " ")).alias("word"))

# Convert all words to lowercase for case-insensitive counting
words_df = words_df.select(lower(col("word")).alias("word"))


#*********************************************************************#
#                Data Cleanup
#*********************************************************************#

#             Remove whitespace and empty words
words_df = words_df.filter(col("word") != "")
words_df = words_df.filter(col("word").rlike(r"\S+"))

# Perform word count by grouping and counting the occurrences of each word
word_count_df = words_df.groupBy("word").count()

# Filter out words with length less than 6
filtered_word_count_df = word_count_df.filter(col("word").rlike(r"\b\w{6,}\b"))

# Sort the filtered word count in descending order
sorted_word_count_df = filtered_word_count_df.orderBy(desc("count"))

# Select the top 100 most frequent words
top_words_df = sorted_word_count_df.limit(100)

# Convert DataFrame to Pandas DataFrame for writing to a file
pandas_df = top_words_df.toPandas()


#*********************************************************************#
#                      Saving the output
#*********************************************************************#

# Write the top words to a file
output_file = "output_file_16GB_input_words_6_more.txt"
pandas_df.to_csv(output_file, index=False)

# Get memory usage after the Spark program has run
memory_usage = get_memory_usage()

# Print the memory usage
print(f"Memory usage:\t\t{memory_usage / 1024 / 1024:.2f} MB")

# Stop the SparkSession
spark.stop()
