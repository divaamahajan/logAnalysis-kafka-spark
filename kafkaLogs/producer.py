import zipfile
from confluent_kafka import Producer

# Specify the Kafka broker and topic to produce to
brokers = 'localhost:9092'
topic = 'log_lines_topic'

# Extract the zip file
dataset = '42MBSmallServerLog.log'
zip_file_path = f'{dataset}.zip'
extracted_folder_path = 'extracted_dataset'


# Set up the Kafka producer
producer = Producer({
    'bootstrap.servers': brokers,  # Kafka broker address
    'api.version.request': True
})

print('Kafka Producer has been initiated...')

def receipt(err,msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        print(message)


def main():
    num = 0
    
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extracted_folder_path)

    # Read log file
    log_file_path = f'{extracted_folder_path}/{dataset}'

    batch_size = 1000  # Adjust this value based on requirements

    # Open the log file
    with open(log_file_path, 'r') as file:
        batch = []
        for line in file:
            # Add each line to the current batch
            batch.append(line)

            # Check if the batch size is reached
            if len(batch) >= batch_size:
                # Produce the batch to Kafka
                producer.produce(topic, value=''.join(batch).encode('utf-8'))
                producer.flush()

                # Clear the batch
                batch = []

        # Check if there are any remaining lines in the last batch
        if batch:
            # Produce the remaining lines to Kafka
            producer.poll(1)
            producer.produce(topic, value=''.join(batch).encode('utf-8'),callback=receipt)
            producer.flush()

    # Flush any remaining messages in the producer queue
    producer.flush()
        
main()