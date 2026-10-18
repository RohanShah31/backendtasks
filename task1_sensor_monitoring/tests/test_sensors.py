"""
Unit and Integration Tests for Task 1: Campus Sensor Monitoring and Alerts
"""

import pytest
import os
import tempfile
from fastapi.testclient import TestClient

# Use temporary SQLite DB for test isolation
test_db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
test_db_path = test_db_file.name
test_db_file.close()

os.environ["SENSORS_DB_PATH"] = test_db_path

from task1_sensor_monitoring.app.main import app, rule_engine
from task1_sensor_monitoring.app.database import init_db


@pytest.fixture(scope="module", autouse=True)
def setup_test_environment():
    init_db()
    rule_engine.alert_cooldown_seconds = 0.0  # Disable cooldown during testing
    yield
    try:
        os.remove(test_db_path)
    except Exception:
        pass


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


def test_health_check(client):
    response = client.get("/api/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"


def test_create_and_list_rules(client):
    new_rule = {
        "name": "Server Room Heat Warning",
        "description": "Alert when server room exceeds 40°C",
        "rule_type": "single",
        "sensor_id": "server-room-1",
        "metric_type": "temperature",
        "operator": ">",
        "threshold": 40.0,
        "enabled": True
    }
    create_resp = client.post("/api/rules", json=new_rule)
    assert create_resp.status_code == 201
    created = create_resp.json()
    assert created["name"] == "Server Room Heat Warning"
    assert created["threshold"] == 40.0
    rule_id = created["id"]

    # Get rule
    get_resp = client.get(f"/api/rules/{rule_id}")
    assert get_resp.status_code == 200
    assert get_resp.json()["id"] == rule_id

    # Toggle rule
    toggle_resp = client.patch(f"/api/rules/{rule_id}/toggle", params={"enabled": False})
    assert toggle_resp.status_code == 200
    assert toggle_resp.json()["enabled"] is False

    # Re-enable
    toggle_resp2 = client.patch(f"/api/rules/{rule_id}/toggle", params={"enabled": True})
    assert toggle_resp2.status_code == 200
    assert toggle_resp2.json()["enabled"] is True


def test_single_reading_triggers_alert(client):
    # Rule for temp > 35.0 exists from default seed
    reading = {
        "sensorId": "library-floor-2",
        "type": "temperature",
        "value": 37.5,
        "timestamp": "2026-10-18T10:00:00Z"
    }
    resp = client.post("/api/readings", json=reading)
    assert resp.status_code == 200
    data = resp.json()
    assert data["accepted"] == 1
    assert data["alerts_triggered"] >= 1

    # Verify alert appears in /api/alerts
    alerts_resp = client.get("/api/alerts?status=OPEN&sensor_id=library-floor-2")
    assert alerts_resp.status_code == 200
    alerts = alerts_resp.json()
    assert len(alerts) > 0
    target_alert = alerts[0]
    assert target_alert["status"] == "OPEN"
    assert target_alert["triggered_value"] >= 35.0

    # Acknowledge alert
    alert_id = target_alert["id"]
    ack_resp = client.patch(f"/api/alerts/{alert_id}/acknowledge", json={"notes": "Investigating AC unit"})
    assert ack_resp.status_code == 200
    assert ack_resp.json()["status"] == "ACKNOWLEDGED"
    assert ack_resp.json()["notes"] == "Investigating AC unit"

    # Resolve alert
    res_resp = client.patch(f"/api/alerts/{alert_id}/resolve", json={"notes": "AC repaired"})
    assert res_resp.status_code == 200
    assert res_resp.json()["status"] == "RESOLVED"


def test_batch_ingestion(client):
    batch = [
        {"sensorId": "lab-1", "type": "temperature", "value": 22.0, "timestamp": "2026-10-18T10:05:00Z"},
        {"sensorId": "lab-2", "type": "temperature", "value": 23.5, "timestamp": "2026-10-18T10:05:01Z"},
        {"sensorId": "lab-3", "type": "temperature", "value": 38.0, "timestamp": "2026-10-18T10:05:02Z"},
    ]
    resp = client.post("/api/readings/batch", json=batch)
    assert resp.status_code == 200
    data = resp.json()
    assert data["accepted"] == 3
    assert data["alerts_triggered"] >= 1


def test_time_window_rule(client):
    # Create time window rule: 10-minute average temperature > 32.0
    rule = {
        "name": "Auditorium High 10m Average Temp",
        "description": "Trigger when 10m average exceeds 32C",
        "rule_type": "time_window",
        "sensor_id": "auditorium-main",
        "metric_type": "temperature",
        "operator": ">",
        "threshold": 32.0,
        "window_minutes": 10,
        "aggregation": "avg",
        "enabled": True
    }
    client.post("/api/rules", json=rule)

    # Ingest readings that average to 34.0 (33, 34, 35)
    batch = [
        {"sensorId": "auditorium-main", "type": "temperature", "value": 33.0, "timestamp": "2026-10-18T10:10:00Z"},
        {"sensorId": "auditorium-main", "type": "temperature", "value": 34.0, "timestamp": "2026-10-18T10:11:00Z"},
        {"sensorId": "auditorium-main", "type": "temperature", "value": 35.0, "timestamp": "2026-10-18T10:12:00Z"},
    ]
    resp = client.post("/api/readings/batch", json=batch)
    assert resp.status_code == 200
    data = resp.json()
    assert data["alerts_triggered"] >= 1
