"""
Campus Sensor Simulator
Simulates continuous or burst telemetry streams from multiple campus locations.
Supports anomaly injection to test rule triggering in real-time.
"""

import time
import random
import argparse
from datetime import datetime, timezone
import requests

CAMPUS_SENSORS = [
    {"sensorId": "library-floor-1", "type": "temperature", "min": 21.0, "max": 26.0, "anomaly_val": 36.5},
    {"sensorId": "library-floor-2", "type": "temperature", "min": 22.0, "max": 27.0, "anomaly_val": 38.0},
    {"sensorId": "library-floor-2", "type": "occupancy", "min": 30.0, "max": 100.0, "anomaly_val": 145.0},
    {"sensorId": "server-room-a", "type": "temperature", "min": 18.0, "max": 23.0, "anomaly_val": 41.2},
    {"sensorId": "server-room-a", "type": "humidity", "min": 40.0, "max": 55.0, "anomaly_val": 85.0},
    {"sensorId": "auditorium-main", "type": "occupancy", "min": 50.0, "max": 300.0, "anomaly_val": 520.0},
    {"sensorId": "auditorium-main", "type": "air_quality", "min": 40.0, "max": 90.0, "anomaly_val": 180.0},
    {"sensorId": "lab-ai-301", "type": "temperature", "min": 20.0, "max": 25.0, "anomaly_val": 37.0},
    {"sensorId": "lab-ai-301", "type": "air_quality", "min": 35.0, "max": 80.0, "anomaly_val": 165.0},
    {"sensorId": "cafeteria-hall", "type": "occupancy", "min": 20.0, "max": 180.0, "anomaly_val": 290.0},
]


def generate_reading(sensor_config: dict, inject_anomaly: bool = False) -> dict:
    """Generates a realistic sensor reading, optionally injecting an anomaly."""
    if inject_anomaly:
        val = sensor_config["anomaly_val"] + round(random.uniform(-1.0, 2.0), 1)
    else:
        val = round(random.uniform(sensor_config["min"], sensor_config["max"]), 1)

    return {
        "sensorId": sensor_config["sensorId"],
        "type": sensor_config["type"],
        "value": val,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def run_continuous(target_url: str, rate: float, duration: int, anomaly_prob: float):
    """Sends readings continuously at specified rate (readings/sec)."""
    print(f"\n[SIMULATOR] Starting continuous telemetry generation:")
    print(f"  Target: {target_url}/api/readings")
    print(f"  Rate: {rate} readings/sec")
    print(f"  Duration: {duration} seconds")
    print(f"  Anomaly Probability: {anomaly_prob * 100:.0f}%\n")

    start_time = time.time()
    total_sent = 0
    total_alerts = 0
    interval = 1.0 / max(rate, 0.1)

    try:
        while time.time() - start_time < duration:
            config = random.choice(CAMPUS_SENSORS)
            is_anomaly = random.random() < anomaly_prob
            reading = generate_reading(config, inject_anomaly=is_anomaly)

            t0 = time.perf_counter()
            try:
                resp = requests.post(f"{target_url}/api/readings", json=reading, timeout=3.0)
                lat_ms = (time.perf_counter() - t0) * 1000.0
                if resp.status_code == 200:
                    data = resp.json()
                    alerts = data.get("alerts_triggered", 0)
                    total_alerts += alerts
                    total_sent += 1
                    status_flag = f"[ALERT TRIGGERED: +{alerts}]" if alerts > 0 else "[OK]"
                    print(f"  [{datetime.now().strftime('%H:%M:%S')}] {reading['sensorId']:<17} | {reading['type']:<12} = {reading['value']:>5.1f} | Latency: {lat_ms:>5.1f}ms {status_flag}")
                else:
                    print(f"  [ERROR] HTTP {resp.status_code}: {resp.text}")
            except Exception as e:
                print(f"  [CONNECTION ERROR] {e}")

            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n[SIMULATOR] Interrupted by user.")

    print(f"\n[SIMULATOR SUMMARY] Finished: Sent {total_sent} readings, Triggered {total_alerts} alerts.")


def run_burst(target_url: str, count: int, batch_size: int = 1000):
    """Sends a batch or burst of readings."""
    print(f"\n[SIMULATOR] Sending burst of {count} readings in batches of {batch_size}...")
    readings = []
    for i in range(count):
        config = random.choice(CAMPUS_SENSORS)
        is_anomaly = (i % 50 == 0)
        readings.append(generate_reading(config, inject_anomaly=is_anomaly))

    t0 = time.perf_counter()
    sent = 0
    alerts = 0
    for i in range(0, count, batch_size):
        chunk = readings[i:i + batch_size]
        resp = requests.post(f"{target_url}/api/readings/batch", json=chunk, timeout=10.0)
        if resp.status_code == 200:
            res_json = resp.json()
            sent += res_json.get("accepted", len(chunk))
            alerts += res_json.get("alerts_triggered", 0)
        else:
            print(f"Error: {resp.status_code} - {resp.text}")

    total_time = time.perf_counter() - t0
    rate = count / total_time if total_time > 0 else 0
    print(f"[SIMULATOR] Burst Complete: {sent} readings sent in {total_time:.3f}s ({rate:,.1f} readings/sec). Alerts: {alerts}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Campus Sensor Telemetry Simulator")
    parser.add_argument("--url", default="http://localhost:8000", help="Target API URL")
    parser.add_argument("--rate", type=float, default=2.0, help="Readings per second for continuous mode")
    parser.add_argument("--duration", type=int, default=15, help="Duration in seconds for continuous mode")
    parser.add_argument("--anomaly-prob", type=float, default=0.15, help="Probability of injecting an anomaly value")
    parser.add_argument("--burst", type=int, default=None, help="If set, sends a burst of N readings and exits")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size for burst mode")

    args = parser.parse_args()

    if args.burst:
        run_burst(args.url, args.burst, args.batch_size)
    else:
        run_continuous(args.url, args.rate, args.duration, args.anomaly_prob)
