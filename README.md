# VCET Hackathon: Smart Factory IoT Pipeline

## 1. Project Overview

This project is a real-time data pipeline built for the VCET Hackathon. It simulates a smart factory environment with 100s of IoT devices, streams the data, processes it for anomalies, and displays the results on a live monitoring dashboard.

---

## 2. Architecture

Our pipeline is built on a modern, scalable, event-driven architecture.



* **Data Source:** A Python script (`iot_simulator.py`) simulates 100 devices and produces data.
* **Message Broker:** Apache Kafka handles the high-velocity stream of sensor data.
* **Data Processor:** A Python consumer (`data_processor.py`) reads from Kafka, analyzes each message for "Normal", "Warning", or "Critical" status.
* **Database:** InfluxDB stores the time-series data efficiently.
* **Dashboard:** Grafana provides a real-time visualization of the factory's status.

---

## 3. How to Run This Project

**Prerequisites:** Docker Desktop, Python 3.9+

#### Step 1: Launch Infrastructure (Person 1's Role)

All services run in Docker. From the root of the project, run:
```bash
docker-compose up -d

Then, create the necessary Kafka topic:
docker exec kafka kafka-topics --create --topic iot_sensor_data --bootstrap-server localhost:9092

Step 2: Start the Data Simulator (Person 2's Role)
Open a new terminal.
cd simulator
pip install -r requirements.txt
python iot_simulator.py

Step 3: Start the Data Processor (Person 3's Role)
Open a third terminal.
cd processor
pip install -r requirements.txt
python data_processor.py

Note: You will need to get the InfluxDB API Token from the InfluxDB UI at http://localhost:8086 and paste it into the script.
Step 4: View the Dashboard (Person 4's Role)

1.	Open Grafana in your browser: http://localhost:3000 (admin/admin).

2.	Configure the InfluxDB data source as per the project guide.

3.	Go to Dashboards -> Import and upload the dashboard/smart_factory_dashboard.json file to instantly load our finished dashboard.

4.	Commit the README: Person 4 will commit and push the final README.md file and create a final pull request.

By completing this "fortification" phase, you will have a project that is not only working but is also secure, stable, and professionally documented. Now you are truly ready to build the "wow factor" features on this solid foundation.
