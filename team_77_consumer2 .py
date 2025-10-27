import json
import csv
from kafka import KafkaConsumer
import os

BROKER_ADDRESS = '10.243.192.13:9092'  # Broker's ZeroTier IP
TOPICS = ['topic-net', 'topic-disk']

NET_FILE = 'net_data1.csv'
DISK_FILE = 'disk_data1.csv'

# Fields required for Network (net_in and net_out)
NET_FIELDS = ['ts', 'server_id', 'net_in', 'net_out']
# Fields required for Disk (disk_io)
DISK_FIELDS = ['ts', 'server_id', 'disk_io']

def append_to_csv(filename, fields, data_dict):
    write_header = not os.path.exists(filename) or os.stat(filename).st_size == 0
    
    try:
        with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields)
            
            if write_header:
                writer.writeheader()
            
            # Filter the incoming dictionary to only include required fields
            row_to_write = {k: v for k, v in data_dict.items() if k in fields}
            writer.writerow(row_to_write)
    except Exception as e:
        print(f"Error writing to file {filename}: {e}")

def run_consumer_2():
    try:
        # 1. Setup Kafka Consumer
        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers=[BROKER_ADDRESS],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            # 2. Deserializer: Decodes JSON from the producer
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print(f"Consumer 2 running and subscribed to {TOPICS}. Broker: {BROKER_ADDRESS}")
        
        # 3. Continuous Consumption Loop
        for message in consumer:
            data = message.value
            topic = message.topic
            
            if topic == 'topic-net':
                # Writes data with fields: ts, server_id, net_in, net_out
                append_to_csv(NET_FILE, NET_FIELDS, data)
                
            elif topic == 'topic-disk':
                # Writes data with fields: ts, server_id, disk_io
                append_to_csv(DISK_FILE, DISK_FIELDS, data)

    except Exception as e:
        print(f"An error occurred in consumer: {e}")

if __name__ == "__main__":
    run_consumer_2()

