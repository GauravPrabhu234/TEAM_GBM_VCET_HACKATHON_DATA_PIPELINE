# === IMPORTS ===
import time
from datetime import datetime, timedelta, timezone
import pandas as pd
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from sklearn.linear_model import LinearRegression

# === CONFIGURATION ===
PERSON_1_IP = "192.168.137.108"
# CRITICAL: Get this token from your team chat. It's the same one Person 3 used.
INFLUX_API_TOKEN = "eQqsK0quoaIUeQFfVYqxTYHq1zBiseK2LCx7o0NX_wc_0Y0vwqk1SeR3svrEWXg8_jH77zRko97I4eEaL-zI7w==" 

INFLUX_URL = f"http://{PERSON_1_IP}:8086"
INFLUX_ORG = "hackathon_org"
INFLUX_BUCKET = "iot_bucket"

# === CLIENT INITIALIZATION ===
# This section connects to the InfluxDB database.
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

    # This Flux query grabs the last 20 minutes of temperature data for more robustness.
    query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -20m) 
          |> filter(fn: (r) => r._measurement == "sensor_metrics" and r.metric_type == "Temperature")
          |> pivot(rowKey:["_time"], columnKey: ["device_id"], valueColumn: "_value")
    '''

    try:
        # The result can be a DataFrame, an empty list, or a list containing DataFrames.
        result = query_api.query_data_frame(query=query, org=INFLUX_ORG)

        df = None
        if isinstance(result, list):
            if not result:
                print("No recent temperature data found to build model (empty list received). Skipping.")
                return
            else:
                df = result[0]
        else:
            df = result

        if df.empty:
            print("No recent temperature data found to build model (empty dataframe received). Skipping.")
            return

    except Exception as e:
        print(f"Error querying data: {e}")
        return

    for device_id in df.columns:
        if device_id in ['result', 'table', '_start', '_stop', '_time']:
            continue

        device_df = df[['_time', device_id]].copy()
        device_df = device_df.dropna()

        device_df[device_id] = pd.to_numeric(device_df[device_id], errors='coerce')
        device_df = device_df.dropna()

        if len(device_df) < 5:
            continue

        X = device_df['_time'].apply(lambda t: t.timestamp()).values.reshape(-1, 1)
        y = device_df[device_id].values

        model = LinearRegression()
        model.fit(X, y)

        # --- IMPROVED: Fixes the DeprecationWarning ---
        future_timestamp = (datetime.now(timezone.utc) + timedelta(minutes=5)).timestamp()
        predicted_temp = model.predict([[future_timestamp]])[0]

        print(f"Device: {device_id} | Current Temp: {y[-1]:.2f}°C | Raw Predicted Temp in 5 mins: {predicted_temp:.2f}°C")

        # --- NEW: Prediction Sanity Check ---
        # We check if the prediction is physically plausible. A real-world system would never be below freezing or absurdly hot.
        if 0 < predicted_temp < 500:
            if predicted_temp > 100.0:
                print(f"  -> 🚨 PREDICTION (VALID): {device_id} is predicted to overheat!")
                point = Point("failure_predictions") \
                    .tag("device_id", device_id) \
                    .field("predicted_temp", predicted_temp)

                write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
                print(f"  -> ✅ Wrote prediction to InfluxDB.")
        else:
            print(f"  -> ⚠ Prediction for {device_id} is unrealistic. Discarding.")


# --- MAIN LOOP ---
while True:
    predict_failures()
    print(f"\nSleeping for 1 minutes before next prediction cycle...")
    time.sleep(60)