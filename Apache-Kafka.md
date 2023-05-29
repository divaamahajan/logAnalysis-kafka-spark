# Kafka

## Download and install Kafka:

```yaml
# Download Kafka
wget https://downloads.apache.org/kafka/3.4.0/kafka-3.4.0-src.tgz

# Extract the downloaded file
tar -xzf kafka-3.4.0-src.tgz

# Rename the extracted folder
mv kafka-3.4.0-src kafka-2.12

# Add Kafka to the PATH in .bashrc
echo "export PATH=/home/$(whoami)/kafka-2.12/bin:\$PATH" >> ~/.bashrc
source ~/.bashrc
```
## Check Kafka Installation
To make sure you have Kafka installed and running, you can follow these steps:
   - Open a terminal or command prompt.
   - Run the command `kafka-topics.sh` (for Unix-based systems) or `kafka-topics.bat` (for Windows systems) to see if the Kafka command-line tools are available. If the command is not found, it means Kafka is not installed.
   
## Start Kafka and ZooKeeper:
   - Once Kafka is installed, you need to start the Kafka server and related services.
   - Open a terminal or command prompt.
   - Navigate to the Kafka installation directory.
   - Start the ZooKeeper service (required for Kafka) by running the following command:

```yaml
# Start the Kafka server
cd kafka-2.12

# Build the Kafka project:
./gradlew jar -PscalaVersion=2.13.10

# After the build process completes successfully, you can start the Kafka server:
bin/kafka-server-start.sh config/server.properties
```
   - Start the ZooKeeper server (in a separate terminal window)
``` yaml
cd kafka-2.12
bin/zookeeper-server-start.sh config/zookeeper.properties
```

   - Kafka should now be up and running on the default settings.

## Create, delete, describe, or change  a Kafka topic:
To create, delete, describe, or change a topic, you can use the `kafka-topics.sh` command. 

```yaml

```

1. To list all available topics, you can run:
   ```yaml
   bin/kafka-topics.sh --list --bootstrap-server localhost:9092
   ```

2. To describe a specific topic, you can run:
   ```yaml
   bin/kafka-topics.sh --describe --topic your_topic_name --bootstrap-server localhost:9092
   ```

3. To create a new topic, you can run:
Replace `your_topic_name` with the desired name for your topic. Adjust the number of partitions (`--partitions`) and replication factor (`--replication-factor`) as per your requirements.
   example :
   ```yaml
    # Create a topic named "your_topic_name" 3 partitions, and a replication factor of 1.
    bin/kafka-topics.sh --create --topic your_topic_name --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
   ```
    ```yaml
    # Create a topic named "your_topic_name" 1 partition, and a replication factor of 1.
    bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic your_topic_name
    ```

4. To delete a topic, you can run:
   ```yaml
   bin/kafka-topics.sh --delete --topic your_topic_name --bootstrap-server localhost:9092
   ```
   Replace `your_topic_name` with the name of the topic you want to delete. Be cautious as this action is irreversible.


## Initialize the Kafka producer and consumer:

```yaml
# Initialize the producer in one terminal window
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic demo_test

# Initialize the consumer in another terminal window
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo_test --from-beginning
```
## To check the address of your Kafka broker:
1. By default, the Kafka broker listens on `localhost` (127.0.0.1) with port `9092`.
2. If you haven't modified the default configuration, the broker address will be `localhost:9092`.
3. You can also check the Kafka configuration file (`server.properties`) located in the Kafka installation directory to verify the `advertised.listeners` property, which specifies the advertised address and port.

# Spark

## Download and install Apache Spark:

```yaml
# Download Spark
wget https://www.apache.org/dyn/closer.lua/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz

# Extract the downloaded file
tar -xzf spark-3.3.2-bin-hadoop3.tgz

# Move Spark to /opt/spark
sudo mv spark-3.3.2-bin-hadoop3 /opt/spark

# Add Spark to the PATH in .bashrc
echo "export SPARK_HOME=/opt/spark/spark-3.3.2-bin-hadoop3" >> ~/.bashrc
echo "export PATH=\$PATH:\$SPARK_HOME/bin:\$SPARK_HOME/sbin" >> ~/.bashrc
source ~/.bashrc
```

## Pyspark
``` yaml
# Install pyspark
pip install pyspark

# Get python path
where python3
```
- This will display the installed path to the Python executable. For example
` /usr/bin/python3`

- Set the `PYSPARK_PYTHON` environment variable to the path of your Python executable. Open a terminal or command prompt and run the following command:
``` yaml
set PYSPARK_PYTHON=/usr/bin/python3
```

# Hive

## Download and install Hive:

```yaml
# Download Hive
wget https://dlcdn.apache.org/hive/hive-3.1.2/apache-hive-3.1.2-bin.tar.gz

# Extract the downloaded file
tar -xzf apache-hive-3.1.2-bin.tar.gz

# Move Hive to /opt
sudo mv apache-hive-3.1.2-bin /opt

# Add Hive to the PATH in .bashrc
echo "export HIVE_HOME=/opt/apache-hive-3.1.2-bin" >> ~/.bashrc
echo "export PATH=\$PATH:\$HIVE_HOME/bin" >> ~/.bashrc
source ~/.bashrc

# Export Hadoop home path
echo "export HADOOP_HOME=/path/to/hadoop" >> /opt/apache-hive-3.1.2-bin/hive-config.sh

# Create Hive directories in HDFS
hdfs dfs -mkdir /tmp
hdfs dfs -chmod g+w /tmp
hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -chmod g+w /user/hive/warehouse

# Initialize the Derby database for Hive
$HIVE_HOME/bin/schematool -dbType derby -initSchema

# Start the Hive terminal
cd $HIVE_HOME/bin
./hive
```
