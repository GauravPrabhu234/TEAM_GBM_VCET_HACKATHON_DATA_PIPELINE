# === IMPORTS ===
import time
from datetime import datetime, timedelta, timezone
import pandas as pd
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from sklearn.linear_model import LinearRegression

# === CONFIGURATION ===
PERSON_1_IP = "192.168.137.108"
INFLUX_API_TOKEN = "eQqsK0quoaIUeQFfVYqxTYHq1zBiseK2LCx7o0NX_wc_0Y0vwqk1SeR3svrEWXg8_jH77zRko97I4eEaL-zI7w==" 
INFLUX_URL = f"http://{PERSON_1_IP}:8086"
INFLUX_ORG = "hackathon_org"
INFLUX_BUCKET = "iot_bucket"

# === CLIENT INITIALIZATION ===
try:
    influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_API_TOKEN, org=INFLUX_ORG)
    query_api = influx_client.query_api()
    write_api = influx_client.write_api(write_options=SYNCHRONOUS)
    print("✅ ML Script: Successfully connected to InfluxDB.")
except Exception as e:
    print(f"❌ ML Script: Could not connect to InfluxDB: {e}")
    exit(1)

def predict_failures():
    """This function queries data, builds a model for each device, and predicts its future temperature."""
    print("\n--- Running Prediction Cycle ---")

    # This Flux query grabs the last 20 minutes of temperature data
    query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -20m) 
          |> filter(fn: (r) => r._measurement == "sensor_metrics" and r.metric_type == "Temperature")
          |> pivot(rowKey:["_time"], columnKey: ["device_id"], valueColumn: "_value")
    '''

    try:
        result = query_api.query_data_frame(query=query, org=INFLUX_ORG)

        if isinstance(result, list):
            if not result:
                print("No recent temperature data found to build model. Skipping.")
                return
            df = pd.concat(result, ignore_index=True)
        else:
            df = result

        if df.empty:
            print("No recent temperature data found to build model. Skipping.")
            return

    except Exception as e:
        print(f"Error querying data: {e}")
        return

    # Loop through each device
    for device_id in df.columns:
        if device_id in ['result', 'table', '_start', '_stop', '_time']:
            continue

        device_df = df[['_time', device_id]].copy().dropna()
        device_df[device_id] = pd.to_numeric(device_df[device_id], errors='coerce')
        device_df = device_df.dropna()

        if len(device_df) < 5:
            continue

        # Prepare features
        X = device_df['_time'].apply(lambda t: t.timestamp()).values.reshape(-1, 1)
        y = device_df[device_id].values

        # Train regression model
        model = LinearRegression()
        model.fit(X, y)

        # Predict 5 minutes into the future
        future_timestamp = (datetime.now(timezone.utc) + timedelta(minutes=5)).timestamp()
        predicted_temp = model.predict([[future_timestamp]])[0]

        print(f"Device: {device_id} | Current Temp: {y[-1]:.2f}°C | Predicted Temp in 5 mins: {predicted_temp:.2f}°C")

        # --- Prediction categorization ---
        if 0 < predicted_temp < 500:
            if predicted_temp <= 75:
                status = "normal"
                print(f"  -> ✅ PREDICTION (NORMAL): {device_id} is safe at {predicted_temp:.2f}°C")
            elif predicted_temp <= 100:
                status = "critical"
                print(f"  -> ⚠ PREDICTION (CRITICAL): {device_id} is high at {predicted_temp:.2f}°C")
            else:
                status = "overheat"
                print(f"  -> 🚨 PREDICTION (OVERHEAT): {device_id} will overheat at {predicted_temp:.2f}°C")

            # Write prediction to InfluxDB
            point = (
                Point("failure_predictions")
                .tag("device_id", device_id)
                .field("predicted_temp", float(predicted_temp))
                .field("status", status)
            )
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
            print(f"  -> ✅ Wrote prediction to InfluxDB.")
        else:
            print(f"  -> ⚠ Prediction for {device_id} is unrealistic. Discarding.")

# --- MAIN LOOP ---
while True:
    predict_failures()
    print(f"\nSleeping for 1 minute before next prediction cycle...")
    time.sleep(60)
