import json
from confluent_kafka import Consumer, KafkaException
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

PERSON_1_IP = "192.168.137.108"
INFLUX_API_TOKEN = "eQqsK0quoaIUeQFfVYqxTYHq1zBiseK2LCx7o0NX_wc_0Y0vwqk1SeR3svrEWXg8_jH77zRko97I4eEaL-zI7w=="

INFLUX_URL = f"http://{PERSON_1_IP}:8086"
INFLUX_ORG = "hackathon_org"
INFLUX_BUCKET = "iot_bucket"

KAFKA_TOPIC = 'iot_sensor_data'
KAFKA_SERVERS = f'{PERSON_1_IP}:9092'
conf = {
    'bootstrap.servers': KAFKA_SERVERS,
    'group.id': 'iot-processor-group-1',
    'auto.offset.reset': 'earliest'
}

def get_status(metric_type, value):
    if metric_type == "Temperature":
        if value > 100.0:return "Critical"
        if value > 80.0:return "Warning"
    elif metric_type == "Current":
        if value > 20.0: return "Critical"
        if value > 15.0:return "Warning"
    elif metric_type == "Pressure":
        if value > 300.0:return "Critical"
        if value > 250.0: return "Warning"
    return "Normal"

print("Initializing clients...")
try:
    influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_API_TOKEN, org=INFLUX_ORG)
    write_api = influx_client.write_api(write_options=SYNCHRONOUS)
    print(f"InfluxDB client configured for {INFLUX_URL}")

    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC])
    print(f"Kafka consumer subscribed to topic '{KAFKA_TOPIC}' at {KAFKA_SERVERS}")
except Exception as e:
    print(f"FATAL: Failed to initialize clients: {e}")
    exit(1)

print("Listening for messages with ANALYTICS...")
try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None: continue
        if msg.error():
            if msg.error().code() != KafkaException._PARTITION_EOF:
                print(f"Kafka Error: {msg.error()}")
            continue

        try:
            data = json.loads(msg.value().decode('utf-8'))
            value = float(data["value"])
            metric_type = data["metric_type"]
            status = get_status(metric_type, value)
            print(f"Received: {data} -> Status: {status}")
            point = Point("sensor_metrics") \
                .tag("device_id", data["device_id"]) \
                .tag("metric_type", metric_type) \
                .tag("status", status) \
                .field("value", value) \
                .time(data["timestamp"])

            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
            print(f"  ->Successfully wrote enriched data to InfluxDB.")

        except Exception as e:
            print(f"  -> ERROR: An error occurred while processing message: {e}")

except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    consumer.close()
    print("Consumer closed.")