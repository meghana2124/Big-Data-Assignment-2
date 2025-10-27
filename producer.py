#!/usr/bin/env python3
"""
High-throughput Producer for team_77 dataset.

- Sends each CSV row as JSON to all four topics: topic-cpu, topic-mem, topic-net, topic-disk
- Timestamps kept exactly as HH:MM:SS (no UTC conversion)
- Default: sends as fast as possible
- Optional --rate for throttling if needed
"""

import json
import time
import argparse
import pandas as pd
from kafka import KafkaProducer

#config
BROKER_ZT_IP = "10.243.192.13:9092"   
DATA_PATH = "dataset.csv"
TOPICS = ["topic-cpu", "topic-mem", "topic-net", "topic-disk"]
# ----------------------------

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=int, default=0,
                        help="Target messages per second across all topics (0 = unlimited).")
    parser.add_argument("--batch-size", type=int, default=500,
                        help="Number of messages per timing batch for rate limiting.")
    parser.add_argument("--flush-every", type=int, default=2000,
                        help="Flush every N messages to ensure delivery.")
    return parser.parse_args()

def json_serializer(obj):
    """Convert Python dict to compact JSON bytes."""
    return json.dumps(obj, separators=(",", ":")).encode("utf-8")

def main():
    args = parse_args()
    print("Producer starting. Broker:", BROKER_ZT_IP)
    print("Rate cap (msgs/sec):", args.rate if args.rate > 0 else "unlimited")

    
    producer = KafkaProducer(
        bootstrap_servers=[BROKER_ZT_IP],
        value_serializer=json_serializer,
        linger_ms=50,
        batch_size=64 * 1024,
        compression_type='gzip',
        acks=1,
        retries=5
    )

    # Load dataset 
    df = pd.read_csv(DATA_PATH)
    total_rows = len(df)
    print(f"Loaded dataset with {total_rows} rows.")

    sent = 0
    batch_counter = 0
    start_time = time.time()

    
    rate = args.rate
    batch_size = max(1, args.batch_size)
    if rate > 0:
        msgs_per_batch = batch_size
        seconds_per_batch = msgs_per_batch / float(rate)
    else:
        msgs_per_batch = None
        seconds_per_batch = None

    batch_start_time = time.time()

    try:
        for _, row in df.iterrows():
            
            record = {
                "ts": str(row['ts']),  
                "server_id": str(row['server_id']),
                "cpu_pct": float(row['cpu_pct']),
                "mem_pct": float(row['mem_pct']),
                "net_in": float(row['net_in']),
                "net_out": float(row['net_out']),
                "disk_io": float(row['disk_io'])
            }

            # send record to all topics
            for topic in TOPICS:
                producer.send(topic, value=record)

            sent += 1
            batch_counter += 1

            
            if rate > 0 and batch_counter >= msgs_per_batch:
                elapsed = time.time() - batch_start_time
                if elapsed < seconds_per_batch:
                    time.sleep(seconds_per_batch - elapsed)
                batch_start_time = time.time()
                batch_counter = 0

            
            if sent % args.flush_every == 0:
                producer.flush()
                elapsed_total = time.time() - start_time
                print(f"[{sent}/{total_rows}] records sent. Elapsed: {elapsed_total:.1f}s")

    except KeyboardInterrupt:
        print("Interrupted by user.")
    finally:
        print("Flushing and closing producer...")
        producer.flush()
        producer.close()
        elapsed = time.time() - start_time
        print(f"Finished. Sent {sent}/{total_rows} records in {elapsed:.1f}s.")

if __name__ == "__main__":
    main()

