"""
Task 2: Workshop Registration and Notifications
FastAPI Application implementing Reliable Asynchronous Notification Architecture
"""

import os
from contextlib import asynccontextmanager
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware

from .models import (
    WorkshopCreate, WorkshopResponse, MemberRegisterRequest,
    RegistrationResponse, NotificationJobResponse
)
from .database import (
    init_db, create_workshop, get_workshops, get_workshop_by_id,
    atomic_register_member, cancel_workshop_and_enqueue_notifications,
    get_all_notifications, WorkshopNotFoundError, WorkshopCancelledError,
    WorkshopFullError, DuplicateRegistrationError
)
from .outbox_worker import OutboxWorker

# Configurable mock notification service URL
MOCK_SERVICE_URL = os.environ.get("MOCK_SERVICE_URL", "http://127.0.0.1:8002/notify")
outbox_worker = OutboxWorker(mock_service_url=MOCK_SERVICE_URL, poll_interval=0.5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initializes SQLite schema and starts outbox worker on boot."""
    init_db()
    outbox_worker.start()
    yield
    await outbox_worker.stop()


app = FastAPI(
    title="Workshop Registration & Reliable Notifications API",
    description="Asynchronous outbox architecture with at-least-once delivery, idempotent retries, and crash recovery.",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# Workshop Management Endpoints
# ============================================================================

@app.post("/api/workshops", response_model=WorkshopResponse, status_code=status.HTTP_201_CREATED, summary="Create a new workshop")
async def create_new_workshop(payload: WorkshopCreate):
    """Allows organizers to create a new workshop with title, description, and capacity."""
    workshop = create_workshop(
        title=payload.title,
        description=payload.description or "",
        capacity=payload.capacity,
        organizer_email=str(payload.organizer_email)
    )
    return workshop


@app.get("/api/workshops", response_model=List[WorkshopResponse], summary="List all workshops")
async def list_all_workshops():
    """Lists all workshops and their current registered attendee counts."""
    return get_workshops()


@app.get("/api/workshops/{workshop_id}", response_model=WorkshopResponse, summary="Get workshop details")
async def get_workshop(workshop_id: str):
    ws = get_workshop_by_id(workshop_id)
    if not ws:
        raise HTTPException(status_code=404, detail="Workshop not found")
    return ws


# ============================================================================
# Member Registration (Immediate Non-Blocking Response)
# ============================================================================

@app.post(
    "/api/workshops/{workshop_id}/register",
    response_model=RegistrationResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register a member for a workshop"
)
async def register_member_for_workshop(workshop_id: str, member: MemberRegisterRequest):
    """
    Atomically registers a member for a workshop.
    Guarantees:
    - Member may register ONLY ONCE per workshop.
    - Returns registration result immediately without waiting for notification delivery.
    - Transactional Outbox pattern guarantees confirmation notification is persisted.
    - If registration fails, no notification is ever created.
    """
    try:
        result = atomic_register_member(
            workshop_id=workshop_id,
            member_name=member.name,
            member_email=str(member.email)
        )
        return result
    except WorkshopNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except WorkshopCancelledError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except WorkshopFullError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except DuplicateRegistrationError as e:
        # Atomic uniqueness constraint violation
        raise HTTPException(status_code=409, detail=str(e))


# ============================================================================
# Workshop Cancellation (Notifies All Registered Members)
# ============================================================================

@app.post("/api/workshops/{workshop_id}/cancel", summary="Cancel workshop and notify all attendees")
async def cancel_workshop(workshop_id: str):
    """
    Cancels a workshop and automatically enqueues cancellation notifications
    for every confirmed member in a single atomic transaction.
    """
    try:
        result = cancel_workshop_and_enqueue_notifications(workshop_id)
        return result
    except WorkshopNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


# ============================================================================
# Outbox Notifications Inspection
# ============================================================================

@app.get("/api/notifications", response_model=List[NotificationJobResponse], summary="Inspect outbox notifications")
async def list_notifications(
    workshop_id: Optional[str] = Query(None, description="Filter by workshop ID"),
    status: Optional[str] = Query(None, description="Filter by status: PENDING, RETRY, DELIVERED, FAILED")
):
    """Inspects the state of the transactional outbox queue."""
    return get_all_notifications(workshop_id=workshop_id, status=status)


@app.get("/api/stats", summary="Get service statistics")
async def service_stats():
    """Summary of workshops, registrations, and notification delivery statuses."""
    notifications = get_all_notifications()
    workshops = get_workshops()
    delivered = sum(1 for n in notifications if n["status"] == "DELIVERED")
    pending = sum(1 for n in notifications if n["status"] == "PENDING")
    retrying = sum(1 for n in notifications if n["status"] == "RETRY")
    failed = sum(1 for n in notifications if n["status"] == "FAILED")

    return {
        "workshops_count": len(workshops),
        "total_notifications_created": len(notifications),
        "delivered_notifications": delivered,
        "pending_notifications": pending,
        "retrying_notifications": retrying,
        "permanently_failed_notifications": failed
    }


@app.get("/api/health", summary="Health check")
async def health_check():
    return {"status": "healthy", "service": "Workshop Registration & Notifications"}
