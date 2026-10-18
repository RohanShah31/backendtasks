"""
Task 1: Campus Sensor Monitoring and Alerts
Database Layer using SQLite with High-Performance WAL mode
"""

import sqlite3
import os
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone
import uuid

DB_PATH = os.environ.get("SENSORS_DB_PATH", "sensors.db")


def get_db_connection() -> sqlite3.Connection:
    """Creates a configured connection with WAL mode and row factory."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # High throughput SQLite configurations
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA cache_size = -64000;")  # 64MB cache
    conn.execute("PRAGMA temp_store = MEMORY;")
    return conn


def init_db():
    """Initializes tables and indexes."""
    conn = get_db_connection()
    try:
        with conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS readings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sensor_id TEXT NOT NULL,
                type TEXT NOT NULL,
                value REAL NOT NULL,
                timestamp TEXT NOT NULL
            );
            """)
            conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_readings_sensor_type_time 
            ON readings(sensor_id, type, timestamp);
            """)
            conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_readings_type_time 
            ON readings(type, timestamp);
            """)

            conn.execute("""
            CREATE TABLE IF NOT EXISTS rules (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                rule_type TEXT NOT NULL,
                sensor_id TEXT,
                metric_type TEXT NOT NULL,
                operator TEXT NOT NULL,
                threshold REAL NOT NULL,
                window_minutes INTEGER,
                aggregation TEXT,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """)

            conn.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id TEXT PRIMARY KEY,
                rule_id TEXT NOT NULL,
                rule_name TEXT NOT NULL,
                sensor_id TEXT NOT NULL,
                metric_type TEXT NOT NULL,
                triggered_value REAL NOT NULL,
                threshold REAL NOT NULL,
                condition_summary TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'OPEN',
                acknowledged_at TEXT,
                resolved_at TEXT,
                notes TEXT
            );
            """)
            conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_alerts_status 
            ON alerts(status, timestamp);
            """)
            conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_alerts_sensor 
            ON alerts(sensor_id, metric_type);
            """)

        # Seed default campus alert rules if table is empty
        _seed_default_rules(conn)
    finally:
        conn.close()


def _seed_default_rules(conn: sqlite3.Connection):
    """Seed initial representative rules if none exist."""
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM rules")
    count = cur.fetchone()[0]
    if count == 0:
        now = datetime.now(timezone.utc).isoformat()
        sample_rules = [
            (
                "rule-temp-critical",
                "Critical Temperature Threshold",
                "Triggers when any sensor reports temperature above 35°C",
                "single",
                None,
                "temperature",
                ">",
                35.0,
                None,
                None,
                1,
                now,
                now
            ),
            (
                "rule-temp-avg-10m",
                "10-Minute Average High Temperature",
                "Triggers when 10-minute average temperature exceeds 30°C",
                "time_window",
                None,
                "temperature",
                ">",
                30.0,
                10,
                "avg",
                1,
                now,
                now
            ),
            (
                "rule-air-quality-poor",
                "Unhealthy Air Quality Index",
                "Triggers when air quality index exceeds 150 (Unhealthy)",
                "single",
                None,
                "air_quality",
                ">",
                150.0,
                None,
                None,
                1,
                now,
                now
            ),
            (
                "rule-library-occupancy",
                "Library High Occupancy",
                "Triggers when library floor 2 occupancy exceeds 120 people",
                "single",
                "library-floor-2",
                "occupancy",
                ">",
                120.0,
                None,
                None,
                1,
                now,
                now
            )
        ]
        with conn:
            conn.executemany("""
            INSERT INTO rules (
                id, name, description, rule_type, sensor_id, metric_type,
                operator, threshold, window_minutes, aggregation, enabled,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, sample_rules)


def bulk_insert_readings(readings: List[Dict[str, Any]]) -> int:
    """Inserts a batch of readings in a single atomic transaction."""
    if not readings:
        return 0
    conn = get_db_connection()
    try:
        tuples = [
            (r["sensorId"], r["type"], float(r["value"]), r["timestamp"])
            for r in readings
        ]
        with conn:
            conn.executemany("""
            INSERT INTO readings (sensor_id, type, value, timestamp)
            VALUES (?, ?, ?, ?)
            """, tuples)
        return len(tuples)
    finally:
        conn.close()


def insert_alert(alert_data: Dict[str, Any]) -> str:
    """Inserts a generated alert into the database."""
    conn = get_db_connection()
    try:
        with conn:
            conn.execute("""
            INSERT INTO alerts (
                id, rule_id, rule_name, sensor_id, metric_type,
                triggered_value, threshold, condition_summary, timestamp,
                status, acknowledged_at, resolved_at, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                alert_data["id"],
                alert_data["rule_id"],
                alert_data["rule_name"],
                alert_data["sensor_id"],
                alert_data["metric_type"],
                alert_data["triggered_value"],
                alert_data["threshold"],
                alert_data["condition_summary"],
                alert_data["timestamp"],
                alert_data.get("status", "OPEN"),
                alert_data.get("acknowledged_at"),
                alert_data.get("resolved_at"),
                alert_data.get("notes")
            ))
        return alert_data["id"]
    finally:
        conn.close()


def get_all_rules() -> List[Dict[str, Any]]:
    """Fetches all rules."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM rules ORDER BY created_at DESC")
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_rule_by_id(rule_id: str) -> Optional[Dict[str, Any]]:
    """Fetches a rule by ID."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM rules WHERE id = ?", (rule_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def insert_rule(rule: Dict[str, Any]) -> Dict[str, Any]:
    """Creates a new rule."""
    conn = get_db_connection()
    try:
        with conn:
            conn.execute("""
            INSERT INTO rules (
                id, name, description, rule_type, sensor_id, metric_type,
                operator, threshold, window_minutes, aggregation, enabled,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                rule["id"],
                rule["name"],
                rule.get("description", ""),
                rule["rule_type"],
                rule.get("sensor_id"),
                rule["metric_type"],
                rule["operator"],
                rule["threshold"],
                rule.get("window_minutes"),
                rule.get("aggregation"),
                1 if rule.get("enabled", True) else 0,
                rule["created_at"],
                rule["updated_at"]
            ))
        return rule
    finally:
        conn.close()


def update_rule(rule_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Updates an existing rule."""
    existing = get_rule_by_id(rule_id)
    if not existing:
        return None

    conn = get_db_connection()
    try:
        set_clauses = []
        params = []
        for key in ["name", "description", "operator", "threshold", "window_minutes", "aggregation", "enabled"]:
            if key in updates and updates[key] is not None:
                set_clauses.append(f"{key} = ?")
                val = 1 if (key == "enabled" and updates[key]) else (0 if key == "enabled" else updates[key])
                params.append(val)
        
        now = datetime.now(timezone.utc).isoformat()
        set_clauses.append("updated_at = ?")
        params.append(now)
        params.append(rule_id)

        sql = f"UPDATE rules SET {', '.join(set_clauses)} WHERE id = ?"
        with conn:
            conn.execute(sql, params)
        return get_rule_by_id(rule_id)
    finally:
        conn.close()


def toggle_rule(rule_id: str, enabled: Optional[bool] = None) -> Optional[Dict[str, Any]]:
    """Toggles or sets the enabled state of a rule."""
    existing = get_rule_by_id(rule_id)
    if not existing:
        return None
    new_state = (not bool(existing["enabled"])) if enabled is None else enabled
    return update_rule(rule_id, {"enabled": new_state})


def delete_rule(rule_id: str) -> bool:
    """Deletes a rule by ID."""
    conn = get_db_connection()
    try:
        with conn:
            cur = conn.execute("DELETE FROM rules WHERE id = ?", (rule_id,))
            return cur.rowcount > 0
    finally:
        conn.close()


def get_alerts(
    status: Optional[str] = None,
    sensor_id: Optional[str] = None,
    metric_type: Optional[str] = None,
    rule_id: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
) -> List[Dict[str, Any]]:
    """Retrieves alerts with filtering and pagination."""
    conn = get_db_connection()
    try:
        query = "SELECT * FROM alerts WHERE 1=1"
        params = []
        if status:
            query += " AND status = ?"
            params.append(status.upper())
        if sensor_id:
            query += " AND sensor_id = ?"
            params.append(sensor_id)
        if metric_type:
            query += " AND metric_type = ?"
            params.append(metric_type)
        if rule_id:
            query += " AND rule_id = ?"
            params.append(rule_id)

        query += " ORDER BY timestamp DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        cur = conn.cursor()
        cur.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_alert_by_id(alert_id: str) -> Optional[Dict[str, Any]]:
    """Retrieves single alert by ID."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM alerts WHERE id = ?", (alert_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def acknowledge_alert(alert_id: str, notes: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Marks an alert as ACKNOWLEDGED."""
    conn = get_db_connection()
    try:
        now = datetime.now(timezone.utc).isoformat()
        with conn:
            if notes is not None:
                cur = conn.execute("""
                UPDATE alerts 
                SET status = 'ACKNOWLEDGED', acknowledged_at = ?, notes = ?
                WHERE id = ?
                """, (now, notes, alert_id))
            else:
                cur = conn.execute("""
                UPDATE alerts 
                SET status = 'ACKNOWLEDGED', acknowledged_at = ?
                WHERE id = ?
                """, (now, alert_id))
            if cur.rowcount == 0:
                return None
        return get_alert_by_id(alert_id)
    finally:
        conn.close()


def resolve_alert(alert_id: str, notes: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Marks an alert as RESOLVED."""
    conn = get_db_connection()
    try:
        now = datetime.now(timezone.utc).isoformat()
        with conn:
            if notes is not None:
                cur = conn.execute("""
                UPDATE alerts 
                SET status = 'RESOLVED', resolved_at = ?, notes = ?
                WHERE id = ?
                """, (now, notes, alert_id))
            else:
                cur = conn.execute("""
                UPDATE alerts 
                SET status = 'RESOLVED', resolved_at = ?
                WHERE id = ?
                """, (now, alert_id))
            if cur.rowcount == 0:
                return None
        return get_alert_by_id(alert_id)
    finally:
        conn.close()


def get_recent_readings(
    sensor_id: Optional[str] = None,
    metric_type: Optional[str] = None,
    limit: int = 50
) -> List[Dict[str, Any]]:
    """Fetches most recent readings."""
    conn = get_db_connection()
    try:
        query = "SELECT * FROM readings WHERE 1=1"
        params = []
        if sensor_id:
            query += " AND sensor_id = ?"
            params.append(sensor_id)
        if metric_type:
            query += " AND type = ?"
            params.append(metric_type)
        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        cur = conn.cursor()
        cur.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_system_stats() -> Dict[str, Any]:
    """Returns database summary statistics."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM readings")
        total_readings = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM rules WHERE enabled = 1")
        active_rules = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM alerts WHERE status = 'OPEN'")
        open_alerts = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM alerts WHERE status = 'ACKNOWLEDGED'")
        acknowledged_alerts = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM alerts WHERE status = 'RESOLVED'")
        resolved_alerts = cur.fetchone()[0]

        return {
            "total_readings_stored": total_readings,
            "active_rules_count": active_rules,
            "open_alerts": open_alerts,
            "acknowledged_alerts": acknowledged_alerts,
            "resolved_alerts": resolved_alerts,
            "total_alerts": open_alerts + acknowledged_alerts + resolved_alerts
        }
    finally:
        conn.close()
