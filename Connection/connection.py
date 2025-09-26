
import json
import random
import time
from datetime import datetime
from confluent_kafka import Producer

PERSON_1_IP = "192.168.137.108"  # connecting to my friend
KAFKA_TOPIC = 'iot_sensor_data'
conf = {'bootstrap.servers': f'{PERSON_1_IP}:9092'}

try:
    producer = Producer(conf)
    print(f"Successfully connected to Kafka at {PERSON_1_IP}:9092.")
except Exception as e:
    print(f"Could not connect to Kafka: {e}. Exiting.")
    exit(1)


DEVICE_IDS = [f"DEVICE-{i:03d}" for i in range(1, 101)]

def get_metric_type(device_id):
    device_num = int(device_id.split('-')[1])
    if 1 <= device_num <= 50:
        return "Current"
    elif 51 <= device_num <= 75:
        return "Temperature"
    else:
        return "Pressure"

# def generate_reading(metric_type):
#     """
#     IMPROVED: Generates Normal, Warning, and Critical values:
#       - Critical: ~5%
#       - Warning: ~10%
#       - Normal: ~85%
#     """
#     rand_val = random.random()
#     if rand_val < 0.05:     
#         state = "Critical"
#     elif rand_val < 0.15:    
#         state = "Warning"
#     else:                    
#         state = "Normal"

#     if metric_type == "Current":
#         if state == "Critical":
#             return random.uniform(20.0, 25.0)
#         if state == "Warning":
#             return random.uniform(15.0, 20.0)
#         return random.uniform(5.0, 15.0)

#     if metric_type == "Temperature":
#         if state == "Critical":
#             return random.uniform(100.0, 120.0)
#         if state == "Warning":
#             return random.uniform(80.0, 100.0)
#         return random.uniform(20.0, 80.0)

#     if metric_type == "Pressure":
#         if state == "Critical":
#             return random.uniform(300.0, 350.0)
#         if state == "Warning":
#             return random.uniform(250.0, 300.0)
#         return random.uniform(100.0, 250.0)

#     return 0.0
def generate_reading(metric_type, device_id): # Note the new device_id parameter
    """
    IMPROVED: Now generates Normal, Warning, and Critical data,
    and makes DEVICE-075 more likely to be critical.
    """
    # --- Special logic for our "problem child" device ---
    if device_id == "DEVICE-075" and metric_type == "Temperature":
        # This device has a 50% chance of being critical, creating a strong trend.
        is_critical = random.random() < 0.5 
        if is_critical:
            return random.uniform(105.0, 115.0)

    # --- Standard logic for all other devices ---
    rand_val = random.random()
    if rand_val < 0.05: # 5% chance of Critical
        state = "Critical"
    elif rand_val < 0.15: # 10% chance of Warning
        state = "Warning"
    else: # 85% chance of Normal
        state = "Normal"

    if metric_type == "Current":
        if state == "Critical": return random.uniform(20.0, 25.0)
        if state == "Warning": return random.uniform(15.0, 20.0)
        return random.uniform(5.0, 15.0)
    elif metric_type == "Temperature":
        if state == "Critical": return random.uniform(100.0, 120.0)
        if state == "Warning": return random.uniform(80.0, 100.0)
        return random.uniform(20.0, 80.0)
    elif metric_type == "Pressure":
        if state == "Critical": return random.uniform(300.0, 350.0)
        if state == "Warning": return random.uniform(250.0, 300.0)
        return random.uniform(100.0, 250.0)
    return 0.0

def delivery_report(err, msg):
    """Callback for message delivery result. Only log errors to reduce noise."""
    if err is not None:
        print(f"Message delivery failed: {err}")

# Main
print(f"Starting to stream data to Kafka topic: '{KAFKA_TOPIC}'...")
msg_count = 0

try:
    while True:
        device_id = random.choice(DEVICE_IDS)
        metric_type = get_metric_type(device_id)
        value = generate_reading(metric_type,device_id)

        message = {
            "device_id": device_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "metric_type": metric_type,
            "value": round(value, 2)
        }

        
        producer.produce(
            KAFKA_TOPIC,
            key=device_id.encode('utf-8'),
            value=json.dumps(message).encode('utf-8'),
            callback=delivery_report
        )

   
        producer.poll(0)
        msg_count += 1

        if msg_count % 100 == 0:
            print(f"Sent {msg_count} messages... flushing.")
            producer.flush()

        time.sleep(0.1)  

except KeyboardInterrupt:
    print("Interrupted by user — flushing remaining messages and exiting.")
    producer.flush()

except Exception as e:
    print(f"An error occurred in main loop: {e}")
    time.sleep(5)


