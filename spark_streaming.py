from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import tensorflow as tf
import numpy as np
import json
from kafka import KafkaProducer
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

spark = SparkSession.builder \
    .appName("KafkaStreaming") \
    .getOrCreate()

sensor_data_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data_topic") \
    .load()

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("sensor_04", FloatType(), True),
    StructField("sensor_06", FloatType(), True),
    StructField("sensor_07", FloatType(), True),
    StructField("sensor_08", FloatType(), True),
    StructField("sensor_09", FloatType(), True)
])

sensor_data_stream = sensor_data_stream.withColumn("value", col("value").cast("string"))
sensor_data_stream = sensor_data_stream.withColumn("value", from_json(col("value"), schema))
sensor_data_stream = sensor_data_stream.selectExpr("value.*")

model = tf.keras.models.load_model('lstm_model.h5')

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def on_send_success(record_metadata):
    print(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")

def on_send_error(excp):
    print(f"Error while sending: {excp}")

def process_stream(batch_df, batch_id):
    if batch_df.isEmpty():
        print("Batch is empty. Skipping processing.")
        return

    sensor_values = [
        row.asDict() for row in batch_df.select(
            'sensor_04', 'sensor_06', 'sensor_07', 'sensor_08', 'sensor_09', 'timestamp'
        ).collect()
    ]

    data = np.array([
        [row['sensor_04'], row['sensor_06'], row['sensor_07'], row['sensor_08'], row['sensor_09']]
        for row in sensor_values
    ])
    data = np.reshape(data, (data.shape[0], 1, data.shape[1]))

    predictions = model.predict(data)
    for idx, prediction in enumerate(predictions):
        result = {
            'timestamp': sensor_values[idx]['timestamp'],
            'prediction': prediction.tolist()
        }
        print(f"Prediction: {result}")
        producer.send('anomaly_detection_topic', result).add_callback(on_send_success).add_errback(on_send_error)


    producer.flush()

query = sensor_data_stream \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()

sensor_data_stream.writeStream \
    .foreachBatch(process_stream) \
    .start() \
    .awaitTermination()
