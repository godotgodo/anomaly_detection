from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
import tensorflow as tf
import numpy as np
import json
from kafka import KafkaProducer

spark = SparkSession.builder \
    .appName("KafkaStreaming") \
    .getOrCreate()

sensor_data_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data_topic") \
    .load()

sensor_data_stream = sensor_data_stream.selectExpr("CAST(value AS STRING)")

sensor_data_stream = sensor_data_stream.selectExpr("json_tuple(value, 'timestamp', 'sensor_04', 'sensor_06', 'sensor_07', 'sensor_08', 'sensor_09') AS (timestamp, sensor_04, sensor_06, sensor_07, sensor_08, sensor_09)")

model = tf.keras.models.load_model('lstm_model.h5')

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def process_stream(batch_df, batch_id):
    sensor_values = batch_df.select('sensor_04', 'sensor_06', 'sensor_07', 'sensor_08', 'sensor_09').collect()

    data = np.array([row[:5] for row in sensor_values])
    data = np.reshape(data, (data.shape[0], 1, data.shape[1]))  # LSTM için uygun şekil

    predictions = model.predict(data)

    for idx, prediction in enumerate(predictions):
        result = {
            'timestamp': sensor_values[idx]['timestamp'],
            'prediction': prediction.tolist()
        }
        print(f"Prediction: {result}")
        producer.send('anomaly_detection_topic', result)
        producer.flush()

sensor_data_stream.writeStream \
    .foreachBatch(process_stream) \
    .start() \
    .awaitTermination()
