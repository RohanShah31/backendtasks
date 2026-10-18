"""
Unit and Integration Tests for Task 2: Workshop Registration & Notifications
"""

import pytest
import os
import tempfile
from fastapi.testclient import TestClient

# Isolated database for tests
test_db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
test_db_path = test_db_file.name
test_db_file.close()

os.environ["WORKSHOP_DB_PATH"] = test_db_path

from task2_workshop_notifications.app.main import app, outbox_worker
from task2_workshop_notifications.app.database import init_db, get_all_notifications


@pytest.fixture(scope="module", autouse=True)
def setup_test_environment():
    init_db()
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
    assert response.json()["status"] == "healthy"


def test_create_and_list_workshops(client):
    ws_data = {
        "title": "Introduction to Kubernetes",
        "description": "Container orchestration fundamentals",
        "capacity": 2,
        "organizer_email": "k8s-lead@vit.ac.in"
    }
    resp = client.post("/api/workshops", json=ws_data)
    assert resp.status_code == 201
    created = resp.json()
    assert created["title"] == "Introduction to Kubernetes"
    assert created["capacity"] == 2
    assert created["status"] == "ACTIVE"
    assert created["registered_count"] == 0


def test_atomic_member_registration(client):
    # Create workshop
    ws = client.post("/api/workshops", json={
        "title": "Machine Learning on Edge",
        "capacity": 5,
        "organizer_email": "edge-ai@vit.ac.in"
    }).json()
    ws_id = ws["id"]

    # Register member
    reg_resp = client.post(f"/api/workshops/{ws_id}/register", json={
        "name": "David Miller",
        "email": "david@vit.ac.in"
    })
    assert reg_resp.status_code == 201
    reg_data = reg_resp.json()
    assert reg_data["status"] == "CONFIRMED"
    assert reg_data["member_email"] == "david@vit.ac.in"
    assert "notification_job_id" in reg_data

    # Verify outbox notification row was created in SQLite
    notifs = client.get(f"/api/notifications?workshop_id={ws_id}").json()
    assert len(notifs) == 1
    assert notifs[0]["member_email"] == "david@vit.ac.in"
    assert notifs[0]["event_type"] == "REGISTRATION_CONFIRMATION"


def test_duplicate_registration_prevented(client):
    ws = client.post("/api/workshops", json={
        "title": "Web3 & Smart Contracts",
        "capacity": 10,
        "organizer_email": "web3@vit.ac.in"
    }).json()
    ws_id = ws["id"]

    reg_payload = {"name": "Emma Watson", "email": "emma@vit.ac.in"}
    resp1 = client.post(f"/api/workshops/{ws_id}/register", json=reg_payload)
    assert resp1.status_code == 201

    # Second registration with same email must fail with 409 Conflict
    resp2 = client.post(f"/api/workshops/{ws_id}/register", json=reg_payload)
    assert resp2.status_code == 409
    assert "already registered" in resp2.json()["detail"].lower()

    # Ensure no duplicate notification was created for failed registration
    notifs = client.get(f"/api/notifications?workshop_id={ws_id}").json()
    assert len(notifs) == 1


def test_workshop_capacity_limit(client):
    ws = client.post("/api/workshops", json={
        "title": "Exclusive VIP Session",
        "capacity": 1,
        "organizer_email": "vip@vit.ac.in"
    }).json()
    ws_id = ws["id"]

    # First registration succeeds
    resp1 = client.post(f"/api/workshops/{ws_id}/register", json={"name": "User 1", "email": "user1@vit.ac.in"})
    assert resp1.status_code == 201

    # Second registration fails due to capacity
    resp2 = client.post(f"/api/workshops/{ws_id}/register", json={"name": "User 2", "email": "user2@vit.ac.in"})
    assert resp2.status_code == 400
    assert "capacity" in resp2.json()["detail"].lower()


def test_workshop_cancellation_notifies_attendees(client):
    ws = client.post("/api/workshops", json={
        "title": "DevOps with Terraform",
        "capacity": 10,
        "organizer_email": "devops@vit.ac.in"
    }).json()
    ws_id = ws["id"]

    # Register 2 attendees
    client.post(f"/api/workshops/{ws_id}/register", json={"name": "Attendee 1", "email": "att1@vit.ac.in"})
    client.post(f"/api/workshops/{ws_id}/register", json={"name": "Attendee 2", "email": "att2@vit.ac.in"})

    # Cancel workshop
    cancel_resp = client.post(f"/api/workshops/{ws_id}/cancel")
    assert cancel_resp.status_code == 200
    cancel_data = cancel_resp.json()
    assert cancel_data["status"] == "CANCELLED"
    assert cancel_data["notified_members_count"] == 2

    # Verify cancellation notifications were created in outbox
    notifs = client.get(f"/api/notifications?workshop_id={ws_id}").json()
    cancellations = [n for n in notifs if n["event_type"] == "WORKSHOP_CANCELLATION"]
    assert len(cancellations) == 2
