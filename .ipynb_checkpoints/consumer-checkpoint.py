from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('sensor_data_topic', 
                         bootstrap_servers='localhost:9092', 
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

for message in consumer:
    sensor_data = message.value
    print(f"Received data: {sensor_data}")
