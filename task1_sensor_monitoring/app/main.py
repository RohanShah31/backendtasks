"""
Task 1: Campus Sensor Monitoring and Alerts
FastAPI Backend Application
"""

import time
import uuid
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import List, Optional, Union, Dict, Any

from fastapi import FastAPI, HTTPException, Query, status, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware

from .models import (
    SensorReading, BatchReadings, RuleCreate, RuleUpdate,
    RuleResponse, AlertResponse, AlertUpdateNotes, IngestResponse
)
from .database import (
    init_db, get_all_rules, get_rule_by_id, insert_rule,
    update_rule, toggle_rule, delete_rule, insert_alert,
    get_alerts, get_alert_by_id, acknowledge_alert, resolve_alert,
    get_recent_readings, get_system_stats
)
from .rule_engine import RuleEngine
from .ingest_queue import IngestQueue

# Global engine and ingestion queue
rule_engine = RuleEngine(alert_cooldown_seconds=10.0)
ingest_queue = IngestQueue(batch_size=5000, flush_interval=0.1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initializes database, rule cache, and background queue worker on startup."""
    # 1. Initialize SQLite schema & default seed rules
    init_db()

    # 2. Load active rules into in-memory engine
    rules = get_all_rules()
    rule_engine.sync_rules(rules)

    # 3. Start background ingestion queue worker
    ingest_queue.start()

    yield

    # Shutdown: flush remaining queue items and stop worker
    await ingest_queue.stop()


app = FastAPI(
    title="Campus Sensor Monitoring & Alerts API",
    description="High-throughput ingestion of campus telemetry with real-time single and time-window alert rule evaluation.",
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
# Telemetry Ingestion Endpoints
# ============================================================================

@app.post(
    "/api/readings",
    response_model=IngestResponse,
    status_code=status.HTTP_200_OK,
    summary="Ingest a single sensor reading"
)
async def ingest_single_reading(reading: SensorReading):
    """
    Ingests an individual sensor reading, evaluates alert rules in-memory,
    and asynchronously persists the reading to SQLite.
    """
    start_time = time.perf_counter()
    reading_dict = {
        "sensorId": reading.sensorId,
        "type": reading.type,
        "value": reading.value,
        "timestamp": reading.timestamp.isoformat()
    }

    # Evaluate rules in-memory
    new_alerts = rule_engine.process_reading(reading_dict)
    for alert in new_alerts:
        insert_alert(alert)

    # Enqueue for non-blocking persistence
    await ingest_queue.enqueue([reading_dict])

    elapsed_ms = (time.perf_counter() - start_time) * 1000.0
    return IngestResponse(
        accepted=1,
        alerts_triggered=len(new_alerts),
        duration_ms=round(elapsed_ms, 3),
        message="Reading ingested and evaluated successfully"
    )


@app.post(
    "/api/readings/batch",
    response_model=IngestResponse,
    status_code=status.HTTP_200_OK,
    summary="Ingest a batch of sensor readings (Burst optimized for 10,000+ readings/sec)"
)
async def ingest_batch_readings(payload: Union[BatchReadings, List[SensorReading]]):
    """
    High-throughput batch ingestion endpoint.
    Accepts up to 10,000+ readings per batch, performs vectorized rule checks
    in memory, buffers records for WAL bulk insertion, and returns sub-50ms.
    """
    start_time = time.perf_counter()

    readings_list = payload.readings if isinstance(payload, BatchReadings) else payload
    total_count = len(readings_list)

    if total_count == 0:
        return IngestResponse(accepted=0, alerts_triggered=0, duration_ms=0.0, message="Empty batch")

    # Fast conversion to native dicts
    converted = [
        {
            "sensorId": r.sensorId,
            "type": r.type,
            "value": r.value,
            "timestamp": r.timestamp.isoformat()
        }
        for r in readings_list
    ]

    # Evaluate rules in memory
    new_alerts = rule_engine.process_batch(converted)
    for alert in new_alerts:
        insert_alert(alert)

    # Enqueue to background persistence worker
    await ingest_queue.enqueue(converted)

    elapsed_ms = (time.perf_counter() - start_time) * 1000.0
    return IngestResponse(
        accepted=total_count,
        alerts_triggered=len(new_alerts),
        duration_ms=round(elapsed_ms, 3),
        message=f"Successfully processed burst of {total_count} readings"
    )


@app.get("/api/readings", summary="Query recent sensor readings")
async def list_recent_readings(
    sensor_id: Optional[str] = Query(None, description="Filter by sensor ID"),
    type: Optional[str] = Query(None, description="Filter by metric type"),
    limit: int = Query(50, ge=1, le=500, description="Max readings to return")
):
    """Retrieves recent readings stored in the database."""
    readings = get_recent_readings(sensor_id=sensor_id, metric_type=type, limit=limit)
    return {"count": len(readings), "readings": readings}


# ============================================================================
# Alert Rules Management Endpoints
# ============================================================================

@app.get("/api/rules", response_model=List[RuleResponse], summary="List all alert rules")
async def list_rules():
    """Lists all configured organizer alert rules."""
    return get_all_rules()


@app.post("/api/rules", response_model=RuleResponse, status_code=status.HTTP_201_CREATED, summary="Create a new alert rule")
async def create_rule(rule_in: RuleCreate):
    """
    Creates an alert rule.
    Supports single-reading threshold rules or time-window aggregation rules.
    """
    if rule_in.rule_type == "time_window":
        if not rule_in.window_minutes or rule_in.window_minutes <= 0:
            raise HTTPException(status_code=400, detail="window_minutes must be > 0 for time_window rules")
        if not rule_in.aggregation:
            raise HTTPException(status_code=400, detail="aggregation (avg, max, min, count) required for time_window rules")

    now = datetime.now(timezone.utc).isoformat()
    rule_dict = {
        "id": f"rule-{uuid.uuid4().hex[:8]}",
        "name": rule_in.name,
        "description": rule_in.description or "",
        "rule_type": rule_in.rule_type,
        "sensor_id": rule_in.sensor_id,
        "metric_type": rule_in.metric_type,
        "operator": rule_in.operator,
        "threshold": rule_in.threshold,
        "window_minutes": rule_in.window_minutes,
        "aggregation": rule_in.aggregation,
        "enabled": rule_in.enabled,
        "created_at": now,
        "updated_at": now
    }

    created = insert_rule(rule_dict)
    # Sync memory cache
    rule_engine.sync_rules(get_all_rules())
    return created


@app.get("/api/rules/{rule_id}", response_model=RuleResponse, summary="Get rule by ID")
async def get_rule(rule_id: str):
    rule = get_rule_by_id(rule_id)
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")
    return rule


@app.put("/api/rules/{rule_id}", response_model=RuleResponse, summary="Update an existing alert rule")
async def edit_rule(rule_id: str, rule_update: RuleUpdate):
    """Updates rule threshold, operator, window, or name."""
    updated = update_rule(rule_id, rule_update.model_dump(exclude_unset=True))
    if not updated:
        raise HTTPException(status_code=404, detail="Rule not found")
    rule_engine.sync_rules(get_all_rules())
    return updated


@app.patch("/api/rules/{rule_id}/toggle", response_model=RuleResponse, summary="Enable or disable an alert rule")
async def toggle_rule_status(rule_id: str, enabled: Optional[bool] = None):
    """Enables or disables an alert rule."""
    toggled = toggle_rule(rule_id, enabled)
    if not toggled:
        raise HTTPException(status_code=404, detail="Rule not found")
    rule_engine.sync_rules(get_all_rules())
    return toggled


@app.delete("/api/rules/{rule_id}", summary="Delete an alert rule")
async def remove_rule(rule_id: str):
    """Deletes an alert rule."""
    success = delete_rule(rule_id)
    if not success:
        raise HTTPException(status_code=404, detail="Rule not found")
    rule_engine.sync_rules(get_all_rules())
    return {"message": f"Rule {rule_id} deleted successfully"}


# ============================================================================
# Alerts Viewing & Management Endpoints
# ============================================================================

@app.get("/api/alerts", response_model=List[AlertResponse], summary="List and filter alerts")
async def list_alerts(
    status: Optional[str] = Query(None, description="Filter by status: OPEN, ACKNOWLEDGED, RESOLVED"),
    sensor_id: Optional[str] = Query(None, description="Filter by sensor ID"),
    metric_type: Optional[str] = Query(None, description="Filter by metric type"),
    rule_id: Optional[str] = Query(None, description="Filter by rule ID"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0)
):
    """Lists triggered alerts with optional filtering by status, sensor, and rule."""
    return get_alerts(
        status=status,
        sensor_id=sensor_id,
        metric_type=metric_type,
        rule_id=rule_id,
        limit=limit,
        offset=offset
    )


@app.get("/api/alerts/{alert_id}", response_model=AlertResponse, summary="Get alert details")
async def get_alert(alert_id: str):
    alert = get_alert_by_id(alert_id)
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    return alert


@app.patch("/api/alerts/{alert_id}/acknowledge", response_model=AlertResponse, summary="Acknowledge an alert")
async def acknowledge_campus_alert(alert_id: str, body: Optional[AlertUpdateNotes] = None):
    """Marks an alert as ACKNOWLEDGED by an organizer."""
    notes = body.notes if body else None
    ack = acknowledge_alert(alert_id, notes=notes)
    if not ack:
        raise HTTPException(status_code=404, detail="Alert not found")
    return ack


@app.patch("/api/alerts/{alert_id}/resolve", response_model=AlertResponse, summary="Resolve an alert")
async def resolve_campus_alert(alert_id: str, body: Optional[AlertUpdateNotes] = None):
    """Marks an alert as RESOLVED by an organizer."""
    notes = body.notes if body else None
    res = resolve_alert(alert_id, notes=notes)
    if not res:
        raise HTTPException(status_code=404, detail="Alert not found")
    return res


# ============================================================================
# System Statistics & Health
# ============================================================================

@app.get("/api/stats", summary="Get system ingestion and alert statistics")
async def system_stats():
    """Returns database summary stats and live queue metrics."""
    db_stats = get_system_stats()
    return {
        **db_stats,
        "in_memory_queue_pending": ingest_queue.queue.qsize() if ingest_queue.queue else 0,
        "total_queued": ingest_queue.total_queued,
        "total_persisted": ingest_queue.total_persisted,
        "cooldown_seconds": rule_engine.alert_cooldown_seconds
    }


@app.get("/api/health", summary="Health check")
async def health_check():
    return {
        "status": "healthy",
        "service": "Campus Sensor Monitoring & Alerts",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
