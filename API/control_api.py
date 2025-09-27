
from fastapi import FastAPI
import uvicorn

app = FastAPI(title="Smart Factory Control API")


@app.get("/api/control/toggle_device")

def toggle_device(device_id: str, action: str):
    """This endpoint simulates sending a control signal to a device via a GET request."""
    print("---")
    print(f"CONTROL SIGNAL RECEIVED:")

    print(f"  -> Device ID: {device_id}")
    print(f"  -> Action:    {action}")
    print(f"  -> SIMULATING RELAY TOGGLE... ACTION LOGGED.")
    print("---\n")
    return {"status": "success", "message": f"'{action}' signal sent to {device_id}"}


if __name__ == "__main__":

    uvicorn.run(app, host="0.0.0.0", port=8001)