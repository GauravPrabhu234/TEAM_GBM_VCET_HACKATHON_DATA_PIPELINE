# from fastapi import FastAPI, HTTPException
# from pydantic import BaseModel
# import uvicorn
# import json
# from confluent_kafka import Producer
# import socket

# # Pydantic Model for incoming data validation
# class SensorData(BaseModel):
#     device_id: str
#     timestamp: str
#     metric_type: str
#     value: float

# # Kafka Producer configuration
# KAFKA_TOPIC = 'iot_sensor_data'
# PERSON_1_IP = "192.168.137.116"
# conf = {
#     'bootstrap.servers': f'{PERSON_1_IP}:9092', # Connect to Kafka via the internal Docker network
#     'client.id': socket.gethostname(),
#     'retries': 5,
#     'acks': 'all'
# }
# producer = Producer(conf)

# # Main FastAPI application
# app = FastAPI(title="IoT Ingestion API")

# def delivery_report(err, msg):
#     """ Callback function for Kafka delivery reports. """
#     if err is not None:
#         print(f"Message delivery failed: {err}")

# @app.post("/ingest")
# def ingest_data(data: SensorData):
#     """ Receives sensor data via POST and produces it to Kafka. """
#     try:
#         producer.produce(
#             KAFKA_TOPIC,
#             key=data.device_id.encode('utf-8'),
#             value=data.model_dump_json().encode('utf-8'),
#             callback=delivery_report
#         )
#         producer.poll(0)
#         return {"status": "success", "message": "Data received and queued for Kafka."}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

# if __name__ == "__main__":
#     # Run on port 8002 to avoid conflict with the control_api
#     uvicorn.run(app, host="0.0.0.0", port=8002)

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
import json
from confluent_kafka import Producer
import socket

# Pydantic Model for incoming data validation
class SensorData(BaseModel):
    device_id: str
    timestamp: str
    metric_type: str
    value: float

# Kafka Producer configuration
KAFKA_TOPIC = 'iot_sensor_data'
PERSON_1_IP = "192.168.137.116"
conf = {
    'bootstrap.servers': f'{PERSON_1_IP}:9092', # Connect to Kafka via the internal Docker network
    'client.id': socket.gethostname(),
    'retries': 5,
    'acks': 'all'
}
producer = Producer(conf)

# Main FastAPI application
app = FastAPI(title="IoT Ingestion API")

def delivery_report(err, msg):
    """ Callback function for Kafka delivery reports. """
    if err is not None:
        print(f"Message delivery failed: {err}")

@app.post("/ingest")
def ingest_data(data: SensorData):
    """ Receives sensor data via POST and produces it to Kafka. """
    try:
        producer.produce(
            KAFKA_TOPIC,
            key=data.device_id.encode('utf-8'),
            value=data.model_dump_json().encode('utf-8'),
            callback=delivery_report
        )
        producer.poll(0)
        return {"status": "success", "message": "Data received and queued for Kafka."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    # Run on port 8002 to avoid conflict with the control_api
    uvicorn.run(app, host="0.0.0.0", port=8002)
