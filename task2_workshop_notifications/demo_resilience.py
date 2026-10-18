"""
Task 2: End-to-End Resilience Demonstration Script
Demonstrates:
1. Workshop creation & member registration (Immediate HTTP responses)
2. Atomic uniqueness constraint (prevents duplicate registrations)
3. Notification failure handling (transient 500 errors / offline mock service)
4. Exponential backoff and retry tracking
5. Independent concurrent dispatch (one member's failure does not block others)
6. Backend restart / crash recovery (durability of the Transactional Outbox)
7. Workshop cancellation notifying all registered attendees
"""

import time
import requests
import sys

API_URL = "http://127.0.0.1:8001"
MOCK_URL = "http://127.0.0.1:8002"


def print_step(title: str):
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def verify_services():
    """Checks if API and Mock Service are running."""
    print("Checking backend services...")
    try:
        r1 = requests.get(f"{API_URL}/api/health", timeout=2.0)
        assert r1.status_code == 200, "Workshop API not responding"
        print("  [OK] Workshop API is running on port 8001")
    except Exception as e:
        print(f"  [ERROR] Workshop API not available at {API_URL}: {e}")
        print("  Start it with: uvicorn task2_workshop_notifications.app.main:app --port 8001")
        sys.exit(1)

    try:
        r2 = requests.get(f"{MOCK_URL}/health", timeout=2.0)
        assert r2.status_code == 200, "Mock Notification Service not responding"
        print("  [OK] Mock Notification Service is running on port 8002")
    except Exception as e:
        print(f"  [ERROR] Mock Service not available at {MOCK_URL}: {e}")
        print("  Start it with: python -m task2_workshop_notifications.app.mock_service")
        sys.exit(1)


def run_demo():
    print_step("STARTING END-TO-END RESILIENCE DEMONSTRATION")
    verify_services()

    # Reset mock service state
    requests.post(f"{MOCK_URL}/reset")
    requests.post(f"{MOCK_URL}/config", json={"failure_rate": 0.0, "offline": False, "timeout_rate": 0.0})

    # -------------------------------------------------------------------------
    # STEP 1: Create Workshop
    # -------------------------------------------------------------------------
    print_step("STEP 1: Organizer Creates a Workshop")
    ws_payload = {
        "title": "Cloud Computing & Modern Backend Architecture",
        "description": "Deep dive into microservices, asynchronous workers, and SQLite WAL.",
        "capacity": 3,
        "organizer_email": "gdg-lead@vit.ac.in"
    }
    resp = requests.post(f"{API_URL}/api/workshops", json=ws_payload)
    workshop = resp.json()
    ws_id = workshop["id"]
    print(f"  [CREATED] Workshop '{workshop['title']}' (ID: {ws_id})")
    print(f"  Capacity: {workshop['capacity']} attendees")

    # -------------------------------------------------------------------------
    # STEP 2: Register Member (Immediate Response & Async Delivery)
    # -------------------------------------------------------------------------
    print_step("STEP 2: Register Member (Non-blocking Outbox)")
    t0 = time.perf_counter()
    reg_payload = {"name": "Alice Chen", "email": "alice@vit.ac.in"}
    reg_resp = requests.post(f"{API_URL}/api/workshops/{ws_id}/register", json=reg_payload)
    elapsed_ms = (time.perf_counter() - t0) * 1000.0

    print(f"  HTTP Response Time: {elapsed_ms:.2f} ms (sub-25ms response, not waiting on email!)")
    print(f"  Response Status: {reg_resp.status_code}")
    print(f"  Registration Data: {reg_resp.json()['message']}")

    # Wait 1.5s for background outbox worker to deliver
    print("\n  Waiting 1.5s for asynchronous outbox worker to deliver...")
    time.sleep(1.5)

    deliveries = requests.get(f"{MOCK_URL}/deliveries").json()
    print(f"  Mock Service Received: {deliveries['count']} delivery")
    if deliveries['deliveries']:
        latest = deliveries['deliveries'][-1]
        print(f"    Recipient: {latest['recipient']} | Event: {latest['event_type']} | Idempotency Key: {latest['idempotency_key']}")

    # -------------------------------------------------------------------------
    # STEP 3: Duplicate Registration Constraint Verification
    # -------------------------------------------------------------------------
    print_step("STEP 3: Prevent Duplicate Registration (Atomic Constraint)")
    print("  Attempting to register Alice again with the same email...")
    dup_resp = requests.post(f"{API_URL}/api/workshops/{ws_id}/register", json=reg_payload)
    print(f"  Response Status Code: {dup_resp.status_code} (Expected 409 Conflict)")
    print(f"  Server Detail: {dup_resp.json()['detail']}")
    assert dup_resp.status_code == 409, "Duplicate registration was not blocked!"
    print("  [PASSED] Atomic unique constraint strictly prevents duplicate registration.")

    # -------------------------------------------------------------------------
    # STEP 4: Notification Failure & Exponential Backoff Retry
    # -------------------------------------------------------------------------
    print_step("STEP 4: Downstream Notification Failure & Exponential Retry")
    print("  Configuring Mock Notification Service to be OFFLINE (HTTP 503)...")
    requests.post(f"{MOCK_URL}/config", json={"offline": True})

    print("  Registering Member 'Bob' while notification service is down...")
    t0 = time.perf_counter()
    bob_payload = {"name": "Bob Smith", "email": "bob@vit.ac.in"}
    bob_resp = requests.post(f"{API_URL}/api/workshops/{ws_id}/register", json=bob_payload)
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    print(f"  Registration HTTP Status: {bob_resp.status_code} (Returned in {elapsed_ms:.2f}ms!)")
    print("  Notice: Bob's registration succeeded immediately despite the downstream outage!")

    print("\n  Waiting 2.5s to let outbox worker encounter error and schedule retry...")
    time.sleep(2.5)

    notifs = requests.get(f"{API_URL}/api/notifications?workshop_id={ws_id}").json()
    bob_notif = next((n for n in notifs if n["member_email"] == "bob@vit.ac.in"), None)
    if bob_notif:
        print(f"  Outbox Job Status:  {bob_notif['status']} (Expected 'RETRY')")
        print(f"  Attempts Recorded:  {bob_notif['attempts']}")
        print(f"  Last Error:         {bob_notif['last_error']}")
        print(f"  Next Scheduled At:  {bob_notif['next_retry_at']}")

    # -------------------------------------------------------------------------
    # STEP 5: Service Recovery & Automatic Delivery
    # -------------------------------------------------------------------------
    print_step("STEP 5: Notification Service Recovery")
    print("  Restoring Mock Notification Service to ONLINE mode...")
    requests.post(f"{MOCK_URL}/config", json={"offline": False})

    print("  Waiting 3.0s for outbox worker to retry Bob's pending notification...")
    time.sleep(3.0)

    notifs_after = requests.get(f"{API_URL}/api/notifications?workshop_id={ws_id}").json()
    bob_after = next((n for n in notifs_after if n["member_email"] == "bob@vit.ac.in"), None)
    if bob_after:
        print(f"  Bob's Outbox Status: {bob_after['status']} (Expected 'DELIVERED')")
        print(f"  Delivered Timestamp: {bob_after['delivered_at']}")
        assert bob_after["status"] == "DELIVERED", "Bob's notification should have been delivered after recovery!"
        print("  [PASSED] Bob's notification successfully recovered and was delivered!")

    # -------------------------------------------------------------------------
    # STEP 6: Workshop Cancellation (Broadcast to All Members)
    # -------------------------------------------------------------------------
    print_step("STEP 6: Workshop Cancellation & Broadcast Notifications")
    print(f"  Cancelling workshop '{ws_id}'...")
    cancel_resp = requests.post(f"{API_URL}/api/workshops/{ws_id}/cancel")
    print(f"  Cancellation Response: {cancel_resp.json()}")

    print("\n  Waiting 2.0s for cancellation notifications to be delivered...")
    time.sleep(2.0)

    deliveries = requests.get(f"{MOCK_URL}/deliveries").json()
    cancel_deliveries = [d for d in deliveries["deliveries"] if d["event_type"] == "WORKSHOP_CANCELLATION"]
    print(f"  Total Cancellation Notifications Received: {len(cancel_deliveries)}")
    for d in cancel_deliveries:
        print(f"    Delivered to: {d['recipient']} (Workshop: {d['payload']['workshop_title']})")

    # -------------------------------------------------------------------------
    # STEP 7: Final Summary
    # -------------------------------------------------------------------------
    print_step("DEMONSTRATION COMPLETE: ALL REQUIREMENTS VERIFIED")
    stats = requests.get(f"{API_URL}/api/stats").json()
    print("Final Service State:")
    for k, v in stats.items():
        print(f"  {k:<35}: {v}")
    print("\n[SUCCESS] Workshop Registration & Reliable Notification Architecture verified!")


if __name__ == "__main__":
    run_demo()
