"""
Task 2: Workshop Registration and Notifications
Database layer with Transactional Outbox Pattern
"""

import sqlite3
import os
import json
import uuid
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone

DB_PATH = os.environ.get("WORKSHOP_DB_PATH", "workshops.db")


class WorkshopError(Exception):
    pass

class WorkshopNotFoundError(WorkshopError):
    pass

class WorkshopCancelledError(WorkshopError):
    pass

class WorkshopFullError(WorkshopError):
    pass

class DuplicateRegistrationError(WorkshopError):
    pass


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    return conn


def init_db():
    conn = get_db()
    try:
        with conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS workshops (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                description TEXT,
                capacity INTEGER NOT NULL,
                organizer_email TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                created_at TEXT NOT NULL,
                cancelled_at TEXT
            );
            """)

            conn.execute("""
            CREATE TABLE IF NOT EXISTS registrations (
                id TEXT PRIMARY KEY,
                workshop_id TEXT NOT NULL,
                member_name TEXT NOT NULL,
                member_email TEXT NOT NULL,
                registered_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'CONFIRMED',
                UNIQUE(workshop_id, member_email),
                FOREIGN KEY (workshop_id) REFERENCES workshops(id)
            );
            """)

            # Transactional Outbox Table
            conn.execute("""
            CREATE TABLE IF NOT EXISTS notifications (
                id TEXT PRIMARY KEY,
                idempotency_key TEXT UNIQUE NOT NULL,
                workshop_id TEXT NOT NULL,
                member_email TEXT NOT NULL,
                member_name TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING',
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 5,
                last_error TEXT,
                next_retry_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                delivered_at TEXT
            );
            """)
            conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_notifications_status_retry 
            ON notifications(status, next_retry_at);
            """)
    finally:
        conn.close()


def create_workshop(title: str, description: str, capacity: int, organizer_email: str) -> Dict[str, Any]:
    conn = get_db()
    try:
        workshop_id = f"ws-{uuid.uuid4().hex[:8]}"
        now = datetime.now(timezone.utc).isoformat()
        with conn:
            conn.execute("""
            INSERT INTO workshops (id, title, description, capacity, organizer_email, status, created_at)
            VALUES (?, ?, ?, ?, ?, 'ACTIVE', ?)
            """, (workshop_id, title, description, capacity, organizer_email, now))
        return get_workshop_by_id(workshop_id)
    finally:
        conn.close()


def get_workshops() -> List[Dict[str, Any]]:
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
        SELECT w.*, COUNT(r.id) as registered_count
        FROM workshops w
        LEFT JOIN registrations r ON w.id = r.workshop_id AND r.status = 'CONFIRMED'
        GROUP BY w.id
        ORDER BY w.created_at DESC
        """)
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_workshop_by_id(workshop_id: str) -> Optional[Dict[str, Any]]:
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
        SELECT w.*, COUNT(r.id) as registered_count
        FROM workshops w
        LEFT JOIN registrations r ON w.id = r.workshop_id AND r.status = 'CONFIRMED'
        WHERE w.id = ?
        GROUP BY w.id
        """, (workshop_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def atomic_register_member(workshop_id: str, member_name: str, member_email: str) -> Dict[str, Any]:
    """
    Atomically registers a member and inserts a pending confirmation notification into the outbox.
    Guarantees:
    1. Member can only register once per workshop.
    2. Notification is saved in the exact same transaction as the registration.
    3. If registration fails or rolls back, no notification is ever created.
    """
    conn = get_db()
    try:
        with conn:
            cur = conn.cursor()
            # 1. Check workshop
            cur.execute("SELECT * FROM workshops WHERE id = ?", (workshop_id,))
            ws = cur.fetchone()
            if not ws:
                raise WorkshopNotFoundError(f"Workshop '{workshop_id}' does not exist.")
            if ws["status"] == "CANCELLED":
                raise WorkshopCancelledError(f"Workshop '{ws['title']}' is cancelled. Registration closed.")

            # 2. Check capacity
            cur.execute("SELECT COUNT(*) FROM registrations WHERE workshop_id = ? AND status = 'CONFIRMED'", (workshop_id,))
            current_count = cur.fetchone()[0]
            if current_count >= ws["capacity"]:
                raise WorkshopFullError(f"Workshop is at full capacity ({ws['capacity']} attendees).")

            # 3. Insert registration (SQLite raises IntegrityError if (workshop_id, member_email) already exists)
            reg_id = f"reg-{uuid.uuid4().hex[:8]}"
            now = datetime.now(timezone.utc).isoformat()
            try:
                cur.execute("""
                INSERT INTO registrations (id, workshop_id, member_name, member_email, registered_at, status)
                VALUES (?, ?, ?, ?, ?, 'CONFIRMED')
                """, (reg_id, workshop_id, member_name, member_email, now))
            except sqlite3.IntegrityError:
                raise DuplicateRegistrationError(f"Member '{member_email}' is already registered for this workshop.")

            # 4. Insert notification into Transactional Outbox
            job_id = f"job-{uuid.uuid4().hex[:8]}"
            idempotency_key = f"reg_conf_{workshop_id}_{member_email.lower().strip()}"
            payload = {
                "event": "REGISTRATION_CONFIRMATION",
                "registration_id": reg_id,
                "workshop_id": workshop_id,
                "workshop_title": ws["title"],
                "member_name": member_name,
                "member_email": member_email,
                "registered_at": now
            }

            cur.execute("""
            INSERT INTO notifications (
                id, idempotency_key, workshop_id, member_email, member_name,
                event_type, payload, status, attempts, max_attempts,
                last_error, next_retry_at, created_at
            ) VALUES (?, ?, ?, ?, ?, 'REGISTRATION_CONFIRMATION', ?, 'PENDING', 0, 5, NULL, ?, ?)
            """, (job_id, idempotency_key, workshop_id, member_email, member_name, json.dumps(payload), now, now))

            return {
                "registration_id": reg_id,
                "workshop_id": workshop_id,
                "workshop_title": ws["title"],
                "member_name": member_name,
                "member_email": member_email,
                "status": "CONFIRMED",
                "message": "Registration successful. Confirmation notification queued for delivery.",
                "registered_at": now,
                "notification_job_id": job_id
            }
    finally:
        conn.close()


def cancel_workshop_and_enqueue_notifications(workshop_id: str) -> Dict[str, Any]:
    """
    Cancels workshop and enqueues cancellation notifications for all registered members
    in a single atomic transaction.
    """
    conn = get_db()
    try:
        with conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM workshops WHERE id = ?", (workshop_id,))
            ws = cur.fetchone()
            if not ws:
                raise WorkshopNotFoundError(f"Workshop '{workshop_id}' does not exist.")
            if ws["status"] == "CANCELLED":
                return {"message": "Workshop was already cancelled.", "notified_members": 0}

            now = datetime.now(timezone.utc).isoformat()
            cur.execute("UPDATE workshops SET status = 'CANCELLED', cancelled_at = ? WHERE id = ?", (now, workshop_id))

            # Fetch all confirmed attendees
            cur.execute("SELECT member_name, member_email FROM registrations WHERE workshop_id = ? AND status = 'CONFIRMED'", (workshop_id,))
            members = cur.fetchall()

            # Enqueue a cancellation notification for every attendee
            queued_jobs = []
            for m in members:
                job_id = f"job-{uuid.uuid4().hex[:8]}"
                idempotency_key = f"cancel_notif_{workshop_id}_{m['member_email'].lower().strip()}"
                payload = {
                    "event": "WORKSHOP_CANCELLATION",
                    "workshop_id": workshop_id,
                    "workshop_title": ws["title"],
                    "member_name": m["member_name"],
                    "member_email": m["member_email"],
                    "cancelled_at": now
                }
                cur.execute("""
                INSERT INTO notifications (
                    id, idempotency_key, workshop_id, member_email, member_name,
                    event_type, payload, status, attempts, max_attempts,
                    last_error, next_retry_at, created_at
                ) VALUES (?, ?, ?, ?, ?, 'WORKSHOP_CANCELLATION', ?, 'PENDING', 0, 5, NULL, ?, ?)
                """, (job_id, idempotency_key, workshop_id, m["member_email"], m["member_name"], json.dumps(payload), now, now))
                queued_jobs.append(job_id)

            return {
                "workshop_id": workshop_id,
                "workshop_title": ws["title"],
                "status": "CANCELLED",
                "notified_members_count": len(members),
                "notification_job_ids": queued_jobs
            }
    finally:
        conn.close()


def get_due_notifications(limit: int = 50) -> List[Dict[str, Any]]:
    """Fetches notifications ready for delivery or retry."""
    conn = get_db()
    try:
        cur = conn.cursor()
        now = datetime.now(timezone.utc).isoformat()
        cur.execute("""
        SELECT * FROM notifications 
        WHERE status IN ('PENDING', 'RETRY') AND next_retry_at <= ?
        ORDER BY next_retry_at ASC
        LIMIT ?
        """, (now, limit))
        results = []
        for row in cur.fetchall():
            d = dict(row)
            d["payload"] = json.loads(d["payload"])
            results.append(d)
        return results
    finally:
        conn.close()


def mark_notification_delivered(job_id: str):
    conn = get_db()
    try:
        now = datetime.now(timezone.utc).isoformat()
        with conn:
            conn.execute("""
            UPDATE notifications 
            SET status = 'DELIVERED', delivered_at = ?, last_error = NULL
            WHERE id = ?
            """, (now, job_id))
    finally:
        conn.close()


def mark_notification_retry(job_id: str, attempts: int, next_retry_at: str, error_msg: str):
    conn = get_db()
    try:
        with conn:
            conn.execute("""
            UPDATE notifications 
            SET status = 'RETRY', attempts = ?, next_retry_at = ?, last_error = ?
            WHERE id = ?
            """, (attempts, next_retry_at, error_msg, job_id))
    finally:
        conn.close()


def mark_notification_failed(job_id: str, attempts: int, error_msg: str):
    conn = get_db()
    try:
        with conn:
            conn.execute("""
            UPDATE notifications 
            SET status = 'FAILED', attempts = ?, last_error = ?
            WHERE id = ?
            """, (attempts, error_msg, job_id))
    finally:
        conn.close()


def get_all_notifications(workshop_id: Optional[str] = None, status: Optional[str] = None) -> List[Dict[str, Any]]:
    conn = get_db()
    try:
        query = "SELECT * FROM notifications WHERE 1=1"
        params = []
        if workshop_id:
            query += " AND workshop_id = ?"
            params.append(workshop_id)
        if status:
            query += " AND status = ?"
            params.append(status.upper())
        query += " ORDER BY created_at DESC"
        cur = conn.cursor()
        cur.execute(query, params)
        results = []
        for row in cur.fetchall():
            d = dict(row)
            d["payload"] = json.loads(d["payload"])
            results.append(d)
        return results
    finally:
        conn.close()
