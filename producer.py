import random
import json
from kafka import KafkaProducer
from datetime import datetime, timedelta
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092', 
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_sensor_data():
    sensor_04 = round(random.uniform(0.2, 1.0), 2)
    sensor_06 = round(random.uniform(0.1, 0.9), 2)
    sensor_07 = round(random.uniform(0.5, 1.2), 2)
    sensor_08 = round(random.uniform(0.3, 1.1), 2)
    sensor_09 = round(random.uniform(0.4, 1.0), 2)

    machine_status = random.choice(['NORMAL', 'BROKEN', 'RECOVERING'])

    timestamp = (datetime.now() - timedelta(minutes=random.randint(0, 10))).strftime('%Y-%m-%d %H:%M:%S')

    sensor_data = {
        'timestamp': timestamp,
        'sensor_04': sensor_04,
        'sensor_06': sensor_06,
        'sensor_07': sensor_07,
        'sensor_08': sensor_08,
        'sensor_09': sensor_09,
        'machine_status': machine_status
    }

    return sensor_data

def send_to_kafka(topic, data):
    producer.send(topic, data)

def produce_data():
    while True:
        sensor_data = generate_sensor_data()
        print(f"Sending data to Kafka: {sensor_data}")
        send_to_kafka('sensor_data_topic', sensor_data)
        time.sleep(1)

produce_data()