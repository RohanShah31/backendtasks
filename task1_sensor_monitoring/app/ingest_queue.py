"""
Task 1: Campus Sensor Monitoring and Alerts
High-Throughput Ingestion Queue & Background Persistence Worker
Enables handling 10,000+ readings/sec without blocking HTTP endpoints.
"""

import asyncio
from typing import List, Dict, Any
import logging
from .database import bulk_insert_readings

logger = logging.getLogger("ingest_queue")


class IngestQueue:
    """
    Asynchronous buffer that batches sensor readings and persists them
    to SQLite in bulk transactions using Write-Ahead Logging (WAL).
    """

    def __init__(self, batch_size: int = 5000, flush_interval: float = 0.1):
        self.queue: asyncio.Queue = None
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._running = False
        self._worker_task: asyncio.Task = None
        self.total_queued = 0
        self.total_persisted = 0

    def _ensure_queue(self):
        if self.queue is None:
            self.queue = asyncio.Queue(maxsize=100000)

    def start(self):
        """Starts the background persistence worker."""
        self._ensure_queue()
        if not self._running:
            self._running = True
            self._worker_task = asyncio.create_task(self._persistence_worker())

    async def stop(self):
        """Stops the worker and flushes remaining queue items."""
        self._running = False
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except (asyncio.CancelledError, Exception):
                pass
            self._worker_task = None
        # Final flush
        await self.flush()
        self.queue = None

    async def enqueue(self, readings: List[Dict[str, Any]]):
        """Enqueues a list of readings into the in-memory buffer."""
        self._ensure_queue()
        for r in readings:
            try:
                self.queue.put_nowait(r)
                self.total_queued += 1
            except asyncio.QueueFull:
                # If buffer is saturated under extreme load, drop or handle gracefully
                pass

    async def flush(self):
        """Drains and writes everything currently in the queue to SQLite."""
        batch = []
        while not self.queue.empty():
            try:
                batch.append(self.queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        if batch:
            # Run blocking SQLite bulk insert in a threadpool to avoid event loop stalls
            await asyncio.to_thread(bulk_insert_readings, batch)
            self.total_persisted += len(batch)

    async def _persistence_worker(self):
        """Background loop draining queue and writing bulk transactions."""
        while self._running:
            batch = []
            try:
                # Wait for at least one item
                first = await asyncio.wait_for(self.queue.get(), timeout=self.flush_interval)
                batch.append(first)

                # Collect up to batch_size items without blocking
                while len(batch) < self.batch_size and not self.queue.empty():
                    batch.append(self.queue.get_nowait())

            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                break

            if batch:
                try:
                    await asyncio.to_thread(bulk_insert_readings, batch)
                    self.total_persisted += len(batch)
                except Exception as e:
                    logger.error(f"Error persisting batch of {len(batch)} readings: {e}")
