# import json
# import random
# import time
# from datetime import datetime
# from kafka import KafkaProducer

# # --- Kafka Producer Configuration ---
# # This will try to connect to the Kafka service that Person 1 is setting up.
# try:
#     producer = KafkaProducer(
#         bootstrap_servers='localhost:9092',
#         value_serializer=lambda v: json.dumps(v).encode('utf-8'),
#         retries=5, # Retry connecting in case Kafka isn't fully ready
#         request_timeout_ms=30000
#     )
#     print("Successfully connected to Kafka.")
# except Exception as e:
#     print(f"Could not connect to Kafka: {e}. Please ensure Docker containers are running. Exiting.")
#     exit(1)


# KAFKA_TOPIC = 'iot_sensor_data'

# # --- Device Simulation Configuration ---
# DEVICE_IDS = [f"DEVICE-{i:03d}" for i in range(1, 101)]

# def get_metric_type(device_id):
#     """Assigns a sensor type based on the device ID number."""
#     device_num = int(device_id.split('-')[1])
#     if 1 <= device_num <= 50:
#         return "Current"
#     elif 51 <= device_num <= 75:
#         return "Temperature"
#     else:
#         return "Pressure"

# def generate_reading(metric_type):
#     """Generates a realistic, and sometimes critical, sensor reading."""
#     is_critical = random.random() < 0.05 # 5% chance of a critical event
#     if metric_type == "Current":
#         return random.uniform(20.0, 25.0) if is_critical else random.uniform(5.0, 15.0)
#     elif metric_type == "Temperature":
#         return random.uniform(100.0, 120.0) if is_critical else random.uniform(20.0, 80.0)
#     elif metric_type == "Pressure":
#         return random.uniform(300.0, 350.0) if is_critical else random.uniform(100.0, 250.0)
#     return 0.0

# # --- Main Loop ---
# print(f"Starting to stream data to Kafka topic: '{KAFKA_TOPIC}'...")
# while True:
#     try:
#         # Pick a random device to send a reading
#         device_id = random.choice(DEVICE_IDS)
#         metric_type = get_metric_type(device_id)
#         value = generate_reading(metric_type)

#         # Construct the message according to our data contract
#         message = {
#             "device_id": device_id,
#             "timestamp": datetime.utcnow().isoformat() + "Z", # ISO 8601 format
#             "metric_type": metric_type,
#             "value": round(value, 2)
#         }

#         # Send the message to Kafka
#         producer.send(KAFKA_TOPIC, value=message)
#         print(f"Sent: {message}")

#         # Wait for a short period before sending the next message
#         time.sleep(0.1)  # ~10 messages per second

#     except Exception as e:
#         print(f"An error occurred: {e}")
#         time.sleep(5) # Wait before retrying if an error occurs


import json
import random
import time
from datetime import datetime
from confluent_kafka import Producer

# --- Configuration ---
PERSON_1_IP = "192.168.137.108"  # <-- Update this to the machine running Kafka
KAFKA_TOPIC = 'iot_sensor_data'

# --- Kafka Producer Configuration ---
conf = {
    'bootstrap.servers': '192.168.137.108:9092'
}



try:
    producer = Producer(conf)
    print(f"✅ Successfully connected to Kafka at {PERSON_1_IP}:9092.")
except Exception as e:
    print(f"❌ Could not connect to Kafka: {e}. Please ensure Docker containers are running. Exiting.")
    exit(1)

# --- Device Simulation Configuration ---
DEVICE_IDS = [f"DEVICE-{i:03d}" for i in range(1, 101)]

def get_metric_type(device_id):
    device_num = int(device_id.split('-')[1])
    if 1 <= device_num <= 50:
        return "Current"
    elif 51 <= device_num <= 75:
        return "Temperature"
    else:
        return "Pressure"

def generate_reading(metric_type):
    is_critical = random.random() < 0.05  # 5% chance
    if metric_type == "Current":
        return random.uniform(20.0, 25.0) if is_critical else random.uniform(5.0, 15.0)
    elif metric_type == "Temperature":
        return random.uniform(100.0, 120.0) if is_critical else random.uniform(20.0, 80.0)
    elif metric_type == "Pressure":
        return random.uniform(300.0, 350.0) if is_critical else random.uniform(100.0, 250.0)
    return 0.0

def delivery_report(err, msg):
    """Callback for message delivery result"""
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

# --- Main Loop ---
print(f"Starting to stream data to Kafka topic: '{KAFKA_TOPIC}'...")
while True:
    try:
        device_id = random.choice(DEVICE_IDS)
        metric_type = get_metric_type(device_id)
        value = generate_reading(metric_type)

        message = {
            "device_id": device_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "metric_type": metric_type,
            "value": round(value, 2)
        }

        # Send the message
        producer.produce(KAFKA_TOPIC, key=device_id, value=json.dumps(message), callback=delivery_report)
        producer.flush()  # ensures message is sent
        time.sleep(0.1)

    except Exception as e:
        print(f"❌ An error occurred: {e}")
        time.sleep(5)
