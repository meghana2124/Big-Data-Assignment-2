import json
import csv
from kafka import KafkaConsumer
import os

BROKER_ADDRESS = '10.243.192.13:9092'
TOPICS = ['topic-cpu', 'topic-mem']

CPU_FILE = 'cpu_data1.csv'
MEM_FILE = 'mem_data1.csv'

CPU_FIELDS = ['ts', 'server_id', 'cpu_pct']
MEM_FIELDS = ['ts', 'server_id', 'mem_pct']

def append_to_csv(filename, fields, data_dict):
    write_header = not os.path.exists(filename) or os.stat(filename).st_size == 0
    
    try:
        with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields)
            
            if write_header:
                writer.writeheader()
            
            row_to_write = {k: v for k, v in data_dict.items() if k in fields}
            writer.writerow(row_to_write)
    except Exception as e:
        print(f"Error writing to file {filename}: {e}")

def run_consumer_1():
    try:
        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers=[BROKER_ADDRESS],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print(f"Consumer 1 running and subscribed to {TOPICS}. Broker: {BROKER_ADDRESS}")
        
        for message in consumer:
            data = message.value
            topic = message.topic
            
            if topic == 'topic-cpu':
                append_to_csv(CPU_FILE, CPU_FIELDS, data)
                
            elif topic == 'topic-mem':
                append_to_csv(MEM_FILE, MEM_FIELDS, data)

    except Exception as e:
        print(f"An error occurred in consumer: {e}")

if __name__ == "__main__":
    run_consumer_1()
