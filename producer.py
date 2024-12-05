import os
import time
import json
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime, timedelta

# Environment variables or default values
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "172.27.73.63:9092")
KAFKA_TOPIC_TEST = os.environ.get("KAFKA_TOPIC_TEST", "vehicle_positions")
KAFKA_API_VERSION = os.environ.get("KAFKA_API_VERSION", "7.3.1")

# Kafka Producer configuration
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    api_version=KAFKA_API_VERSION,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')  # Automatically serialize to JSON
)

def send_records(csv_file_path):
    # Load CSV data using pandas
    df = pd.read_csv(csv_file_path)
    print("Time Started: " + datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    start_time = datetime.now()

    # Process each unique 't' value
    for t in df['t'].unique():
        subset = df[df['t'] == t]
        # Iterate over each row in the subset to prepare records list
        for index, row in subset.iterrows():
            # Calculate the new time including the 't' delta        
            delta_time = timedelta(seconds=int(row['t']))
            adjusted_time = start_time + delta_time

            message = {
                "name": str(row['name']),
                "origin": row['orig'],
                "destination": row['dest'],
                "time": adjusted_time.strftime("%d/%m/%Y %H:%M:%S"),
                "link": row['link'],
                "position": row['x'],
                "spacing": row['s'],
                "speed": row['v']
            }
            # Send the collected records as a single message
            producer.send(KAFKA_TOPIC_TEST, message)
            print("Sent Message for delta " + str(row['t']))
            producer.flush()  # Ensure message is sent     
        time.sleep(0.1)

send_records("sim_data.csv")