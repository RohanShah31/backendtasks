"""
Task 2: Asynchronous Outbox Notification Worker
Reliable background worker implementing:
- Transactional Outbox pattern polling
- Independent concurrent dispatch (failure for one member never blocks others)
- Exponential backoff with jitter
- At-least-once delivery with idempotency keys
- Crash recovery across backend restarts
"""

import asyncio
import random
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any
import httpx

from .database import (
    get_due_notifications,
    mark_notification_delivered,
    mark_notification_retry,
    mark_notification_failed
)

logger = logging.getLogger("outbox_worker")


class OutboxWorker:
    """
    Background worker that reliably delivers notifications from the SQLite outbox.
    """

    def __init__(
        self,
        mock_service_url: str = "http://127.0.0.1:8002/notify",
        poll_interval: float = 0.5,
        concurrency: int = 10,
        base_backoff_sec: float = 1.0,
        max_backoff_sec: float = 30.0,
        request_timeout_sec: float = 3.0
    ):
        self.mock_service_url = mock_service_url
        self.poll_interval = poll_interval
        self.semaphore = asyncio.Semaphore(concurrency)
        self.base_backoff_sec = base_backoff_sec
        self.max_backoff_sec = max_backoff_sec
        self.request_timeout_sec = request_timeout_sec
        self._running = False
        self._worker_task: Optional[asyncio.Task] = None
        self._active_jobs = set()

    def start(self):
        """Starts the background worker loop."""
        if not self._running:
            self._running = True
            self._worker_task = asyncio.create_task(self._poll_loop())
            logger.info("Outbox worker started.")

    async def stop(self):
        """Gracefully stops the worker."""
        self._running = False
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except (asyncio.CancelledError, Exception):
                pass
            self._worker_task = None
        logger.info("Outbox worker stopped.")

    async def _poll_loop(self):
        """Continuously polls SQLite for pending/retry notifications."""
        async with httpx.AsyncClient(timeout=self.request_timeout_sec) as client:
            while self._running:
                try:
                    # Query outbox for due notifications in a thread to keep loop free
                    due_jobs = await asyncio.to_thread(get_due_notifications, limit=20)

                    for job in due_jobs:
                        job_id = job["id"]
                        if job_id not in self._active_jobs:
                            self._active_jobs.add(job_id)
                            # Dispatch job concurrently (independent from other members)
                            asyncio.create_task(self._dispatch_with_semaphore(client, job))

                except Exception as e:
                    logger.error(f"Error in outbox poll loop: {e}")

                await asyncio.sleep(self.poll_interval)

    async def _dispatch_with_semaphore(self, client: httpx.AsyncClient, job: Dict[str, Any]):
        """Dispatches a single notification under concurrency control."""
        try:
            async with self.semaphore:
                await self._deliver_single_job(client, job)
        finally:
            self._active_jobs.discard(job["id"])

    async def _deliver_single_job(self, client: httpx.AsyncClient, job: Dict[str, Any]):
        """
        Attempts HTTP delivery to mock notification service.
        Updates state in SQLite based on success or failure.
        """
        job_id = job["id"]
        headers = {
            "X-Idempotency-Key": job["idempotency_key"],
            "Content-Type": "application/json"
        }
        payload = job["payload"]

        try:
            resp = await client.post(
                self.mock_service_url,
                json=payload,
                headers=headers
            )

            # Success: HTTP 200 / 201
            if resp.status_code in (200, 201):
                await asyncio.to_thread(mark_notification_delivered, job_id)
                logger.info(f"Delivered notification {job_id} to {job['member_email']}")
                return

            # Mock service returned HTTP error (e.g. 500 / 503)
            error_reason = f"HTTP {resp.status_code}: {resp.text[:120]}"
            await self._handle_failure(job, error_reason)

        except (httpx.TimeoutException, httpx.NetworkError, Exception) as exc:
            # Network timeout or connection refused
            error_reason = f"Network failure: {type(exc).__name__} ({str(exc)[:100]})"
            await self._handle_failure(job, error_reason)

    async def _handle_failure(self, job: Dict[str, Any], error_reason: str):
        """Computes exponential backoff and schedules retry or marks failed."""
        job_id = job["id"]
        attempts = job["attempts"] + 1
        max_attempts = job["max_attempts"]

        if attempts >= max_attempts:
            # Dead-letter / exhausted retries
            await asyncio.to_thread(mark_notification_failed, job_id, attempts, error_reason)
            logger.warning(f"Notification {job_id} exhausted max retries ({max_attempts}). Marked FAILED.")
        else:
            # Exponential backoff formula: base * 2^(attempt-1) + jitter
            backoff_base = self.base_backoff_sec * (2 ** (attempts - 1))
            jitter = random.uniform(0.1, 0.5)
            delay = min(self.max_backoff_sec, backoff_base + jitter)

            next_retry = datetime.now(timezone.utc) + timedelta(seconds=delay)
            next_retry_str = next_retry.isoformat()

            await asyncio.to_thread(
                mark_notification_retry,
                job_id,
                attempts,
                next_retry_str,
                error_reason
            )
            logger.info(
                f"Notification {job_id} failed (attempt {attempts}/{max_attempts}): {error_reason}. "
                f"Retrying in {delay:.2f}s at {next_retry_str}"
            )
