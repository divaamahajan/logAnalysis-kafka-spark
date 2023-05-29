# Spark-Logs
1. Navigate to the directory where the log_analysis.py file is located using the cd command. For example:
``` yaml
cd /path/to/directory
```

2. Make sure you have Spark and PySpark installed and properly configured on your machine.

3. Download outputfile
```yaml
Invoke-WebRequest -Uri "ftp://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz" -OutFile "C:\Users\Divya Mahajan\OneDrive\Documents\gitcode\Spark-Logs\NASA_access_log_Aug95.gz"
Invoke-WebRequest -Uri "ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz" -OutFile "C:\Users\Divya Mahajan\OneDrive\Documents\gitcode\Spark-Logs\NASA_access_log_Jul95.gz"
```
3. Run the Python script using the `spark-submit` command, which is used to submit Spark applications. The command should be:
``` yaml
spark-submit log_analysis.py
```
This will execute the script and run it using Spark.

4. The script will process the log data and display the desired output in the terminal.4

# Kafka Logs
To test the code via terminal, you can follow these steps:

1. go to the directory where git code is cloned in directory `kafkaLogs`.

2. Install dependencies:
   - In your terminal, install the required dependencies by running the following commands:
     ``` yaml
     pip install requirements.txt
     ```

3. Extract the zip file:
   - Place the log file (`42MBSmallServerLog.log`) and the corresponding zip file in the same directory as the Python files.
   - Extract the zip file by running the following command in the terminal:
     ``` yaml
     unzip 42MBSmallServerLog.log.zip
     ```

4. Set up Kafka topic and broker:
   - Ensure that Kafka is installed and running on your local machine.
   - Specify the Kafka broker address and the topic to consume from by modifying the `producer.py` and `consumer.py` files accordingly.

``` yaml
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic log_lines_topic
```

5. Run the Kafka producer:
   - In the terminal, run the `producer.py` file to publish the log data to the Kafka topic:
     ``` yaml
     python3 producer.py
     ```

6. Run the Kafka consumer with Spark:
   - Open a new terminal window.
   - In the new terminal, run the `consumer.py` file to consume the log data from the Kafka topic and perform transformations using Spark:
     ``` yaml
     python3 consumer.py
     ```

7. Create Parquet files:
   - Open another terminal window.
   - In the new terminal, run the `parquet_creation.py` file to create Parquet files from the transformed data:
     ``` yaml
     python3 parquet_creation.py
     ```

8. Store Parquet files in HDFS:
   - In the same terminal window, run the `hdfs_storage.py` file to store the Parquet files in HDFS:
     ``` yaml
     python3 hdfs_storage.py
     ```

By following these steps, you will be able to test the code flow that begins with a Kafka producer, followed by a Kafka consumer performing transformations using Spark. The transformed data will be converted into Parquet file format and stored in HDFS.
