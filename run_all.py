"""
Master Command Runner for GDG Tech Backend Tasks
Provides convenient CLI commands to run servers, simulators, load tests, and demos.
"""

import sys
import subprocess
import os

USAGE = """
GDG Tech Wildcard Entry - Backend Solutions CLI

Usage:
  python run_all.py <command>

Available Commands:
  test          Run all unit and integration test suites
  task1         Start Task 1: Campus Sensor Monitoring Service (port 8000)
  simulator     Run Task 1 Sensor Telemetry Simulator
  loadtest      Run Task 1 Burst Load Test (10,000 readings in 1s)
  mock2         Start Task 2 Mock Notification Service (port 8002)
  task2         Start Task 2 Workshop Registration Service (port 8001)
  demo2         Run Task 2 Resilience & Recovery Demonstration
"""

def main():
    if len(sys.argv) < 2:
        print(USAGE)
        return

    cmd = sys.argv[1].lower().strip()

    if cmd == "test":
        print("\n[RUNNING ALL TESTS]")
        subprocess.run([sys.executable, "-m", "pytest", "task1_sensor_monitoring/tests", "task2_workshop_notifications/tests", "-v"])

    elif cmd == "task1":
        print("\n[STARTING TASK 1: SENSOR MONITORING SERVICE (Port 8000)]")
        print("API Documentation: http://127.0.0.1:8000/docs")
        subprocess.run([sys.executable, "-m", "uvicorn", "task1_sensor_monitoring.app.main:app", "--port", "8000", "--reload"])

    elif cmd == "simulator":
        print("\n[STARTING TASK 1: SENSOR SIMULATOR]")
        subprocess.run([sys.executable, "task1_sensor_monitoring/simulator.py", "--rate", "5", "--duration", "20"])

    elif cmd == "loadtest":
        print("\n[STARTING TASK 1: BURST LOAD TEST (10,000 Readings in 1s)]")
        subprocess.run([sys.executable, "task1_sensor_monitoring/load_test.py", "--readings", "10000", "--concurrency", "10"])

    elif cmd == "mock2":
        print("\n[STARTING TASK 2: MOCK NOTIFICATION SERVICE (Port 8002)]")
        print("Mock API Docs: http://127.0.0.1:8002/docs")
        subprocess.run([sys.executable, "-m", "uvicorn", "task2_workshop_notifications.app.mock_service:app", "--port", "8002", "--reload"])

    elif cmd == "task2":
        print("\n[STARTING TASK 2: WORKSHOP REGISTRATION SERVICE (Port 8001)]")
        print("Workshop API Docs: http://127.0.0.1:8001/docs")
        subprocess.run([sys.executable, "-m", "uvicorn", "task2_workshop_notifications.app.main:app", "--port", "8001", "--reload"])

    elif cmd == "demo2":
        print("\n[STARTING TASK 2: RESILIENCE DEMONSTRATION]")
        subprocess.run([sys.executable, "task2_workshop_notifications/demo_resilience.py"])

    else:
        print(f"Unknown command: '{cmd}'")
        print(USAGE)


if __name__ == "__main__":
    main()
