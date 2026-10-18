"""
Task 2: Mock HTTP Notification Service
Simulates a real-world third-party notification provider (Email/SMS).
Supports configurable failure rates, timeouts, latency, and idempotency tracking.
"""

import asyncio
import random
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, Header, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(
    title="Mock HTTP Notification Service",
    description="Simulates an external notification provider with configurable errors and timeouts",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class MockConfigModel(BaseModel):
    failure_rate: float = 0.0
    timeout_rate: float = 0.0
    latency_seconds: float = 0.02
    offline: bool = False


# In-memory mock service state
state = {
    "config": {
        "failure_rate": 0.0,
        "timeout_rate": 0.0,
        "latency_seconds": 0.02,
        "offline": False
    },
    "deliveries": [],
    "idempotency_seen": set(),
    "duplicate_attempts": 0,
    "total_requests": 0,
    "failed_requests": 0
}


@app.post("/notify", summary="Mock dispatch endpoint")
async def send_notification(
    payload: Dict[str, Any],
    x_idempotency_key: Optional[str] = Header(None)
):
    """
    Receives notification payloads.
    Simulates real-world flakiness based on dynamic configuration.
    """
    state["total_requests"] += 1
    cfg = state["config"]

    # 1. Total service outage
    if cfg["offline"]:
        state["failed_requests"] += 1
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Mock Notification Service is currently offline (simulated outage)"
        )

    # 2. Simulated artificial latency
    if cfg["latency_seconds"] > 0:
        await asyncio.sleep(cfg["latency_seconds"])

    # 3. Simulated timeout
    if cfg["timeout_rate"] > 0 and random.random() < cfg["timeout_rate"]:
        state["failed_requests"] += 1
        # Sleep long enough to trigger client HTTP timeout
        await asyncio.sleep(8.0)
        raise HTTPException(status_code=status.HTTP_504_GATEWAY_TIMEOUT, detail="Simulated gateway timeout")

    # 4. Simulated transient HTTP 500 error
    if cfg["failure_rate"] > 0 and random.random() < cfg["failure_rate"]:
        state["failed_requests"] += 1
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Simulated downstream MTA delivery failure (transient error)"
        )

    # 5. Idempotency Check
    is_duplicate = False
    if x_idempotency_key:
        if x_idempotency_key in state["idempotency_seen"]:
            is_duplicate = True
            state["duplicate_attempts"] += 1
        else:
            state["idempotency_seen"].add(x_idempotency_key)

    now = datetime.now(timezone.utc).isoformat()
    record = {
        "idempotency_key": x_idempotency_key,
        "recipient": payload.get("member_email"),
        "event_type": payload.get("event"),
        "payload": payload,
        "received_at": now,
        "is_duplicate": is_duplicate
    }

    if not is_duplicate:
        state["deliveries"].append(record)

    return {
        "status": "DELIVERED",
        "delivered_at": now,
        "is_duplicate_ignored": is_duplicate,
        "recipient": payload.get("member_email"),
        "message": "Notification received and processed successfully"
    }


@app.get("/deliveries", summary="List all delivered notifications")
async def get_deliveries():
    return {
        "count": len(state["deliveries"]),
        "duplicate_prevented_count": state["duplicate_attempts"],
        "total_requests_received": state["total_requests"],
        "failed_requests": state["failed_requests"],
        "deliveries": state["deliveries"]
    }


@app.get("/config", summary="Get current mock configuration")
async def get_config():
    return state["config"]


@app.post("/config", summary="Update mock behavior (failure rate, latency, offline)")
async def update_config(new_config: MockConfigModel):
    state["config"].update(new_config.model_dump())
    return {
        "message": "Mock notification service configuration updated",
        "config": state["config"]
    }


@app.post("/reset", summary="Reset mock state")
async def reset_state():
    state["deliveries"] = []
    state["idempotency_seen"] = set()
    state["duplicate_attempts"] = 0
    state["total_requests"] = 0
    state["failed_requests"] = 0
    return {"message": "Mock service state reset successfully"}


@app.get("/health", summary="Health check")
async def health_check():
    return {"status": "ok", "service": "Mock Notification Provider"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8002)
