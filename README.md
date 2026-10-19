# 🚀 GDG Tech Wildcard Entry — Backend Development Suite

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.13-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python 3.13" />
  <img src="https://img.shields.io/badge/FastAPI-0.116-009688?style=for-the-badge&logo=fastapi&logoColor=white" alt="FastAPI" />
  <img src="https://img.shields.io/badge/SQLite-WAL_Mode-003B57?style=for-the-badge&logo=sqlite&logoColor=white" alt="SQLite WAL" />
  <img src="https://img.shields.io/badge/Tests-11%20Passed-brightgreen?style=for-the-badge&logo=pytest&logoColor=white" alt="Tests 11 Passed" />
  <img src="https://img.shields.io/badge/License-MIT-blue?style=for-the-badge" alt="MIT License" />
  <img src="https://img.shields.io/badge/GDG%20VIT%20Vellore-Wildcard%20Entry-EA4335?style=for-the-badge&logo=google&logoColor=white" alt="GDG VIT Vellore" />
</p>

> **Comprehensive, production-ready solution for both Backend Challenge Tasks:**
> 1. **Task 1: Campus Sensor Monitoring & Alerts** — High-throughput ingestion of **10,000+ readings/second**, in-memory sliding time-window rule evaluator, telemetry simulator, and burst load-test report.
> 2. **Task 2: Workshop Registration & Notifications** — Non-blocking registration, **Transactional Outbox Pattern**, crash recovery, independent concurrent workers with exponential backoff & jitter, and idempotency tracking.

---

## 📑 Table of Contents

- [Key Architecture Highlights](#-key-architecture-highlights)
- [Repository Structure](#-repository-structure)
- [Quick Start Guide](#-quick-start-guide)
- [Task 1: Campus Sensor Monitoring and Alerts](#-task-1-campus-sensor-monitoring-and-alerts)
  - [Architecture & 10,000 Readings/Sec Ingestion](#architecture--10000-readingssec-ingestion)
  - [Sliding Window Rule Evaluation](#sliding-window-rule-evaluation)
  - [Verified Load Test Results (10,000 burst in 0.93s)](#verified-load-test-results)
  - [Telemetry Simulator](#telemetry-simulator)
- [Task 2: Workshop Registration and Notifications](#-task-2-workshop-registration-and-notifications)
  - [The Transactional Outbox Pattern](#the-transactional-outbox-pattern)
  - [Handling Failures, Restarts & Duplicate Delivery](#handling-failures-restarts--duplicate-delivery)
  - [Interactive Resilience Demonstration](#interactive-resilience-demonstration)
- [Automated Test Suite (11/11 Passing)](#-automated-test-suite)
- [Interview Explanation Guide (How to Explain the Entire Project in 3 Minutes)](#-interview-explanation-guide)

---

## 💡 Key Architecture Highlights

| Design Goal | Our Implementation | Why It Matters |
| :--- | :--- | :--- |
| **High Throughput (10k/s)** | In-memory evaluation + asynchronous SQLite WAL write-behind queue | Sub-50ms HTTP response times under heavy burst without database thread-locking. |
| **Sliding Aggregations** | Circular ring buffers (`collections.deque`) in memory | $O(1)$ amortized window pruning for rolling 10-minute averages. |
| **Alert Flooding Prevention** | Per-sensor / per-rule cooldown hashing | Prevents database exhaustion from thousands of duplicate alerts during spikes. |
| **Zero Lost Notifications** | **Transactional Outbox Pattern** | Registration and outbox notification jobs commit in the exact same DB transaction. |
| **Crash Durability** | SQLite on-disk outbox state | Pending jobs survive backend kills / reboots and automatically resume on boot. |
| **Fault Isolation** | Independent concurrent `asyncio.Semaphore` dispatch | Member A's downstream failure or slow timeout never delays Member B. |
| **Duplicate Delivery Guard** | `X-Idempotency-Key` headers | Retries of already-received notifications are deduplicated safely. |

---

## 📁 Repository Structure

```
gdg-backend-wildcard/
├── task1_sensor_monitoring/
│   ├── app/
│   │   ├── main.py              # FastAPI endpoints (/api/readings, /api/rules, /api/alerts)
│   │   ├── models.py            # Pydantic data schemas
│   │   ├── database.py          # SQLite WAL layer, bulk executemany, alert queries
│   │   ├── rule_engine.py       # In-memory evaluation & sliding circular deques
│   │   └── ingest_queue.py      # Non-blocking write-behind persistence buffer
│   ├── simulator.py             # Realistic campus telemetry generator with anomaly injection
│   ├── load_test.py             # Firing 10,000 readings in 1s & latency percentile metrics
│   ├── LOAD_TEST_REPORT.md      # Auto-generated load test results & throughput analysis
│   ├── ARCHITECTURE.md          # Architectural decisions & throughput trade-offs
│   └── tests/
│       └── test_sensors.py      # Test suite for ingestion, rules, and alert lifecycle
│
├── task2_workshop_notifications/
│   ├── app/
│   │   ├── main.py              # Workshop & Registration API endpoints
│   │   ├── models.py            # Pydantic schemas (Workshop, Member, Notification)
│   │   ├── database.py          # SQLite transactional outbox & atomic constraints
│   │   ├── outbox_worker.py     # Independent async worker with exponential backoff & jitter
│   │   └── mock_service.py      # Mock notification HTTP server (simulates 500s & timeouts)
│   ├── demo_resilience.py       # Interactive end-to-end resilience demonstration
│   ├── ARCHITECTURE.md          # In-depth guide on outbox, retries, idempotency & recovery
│   └── tests/
│       └── test_workshop.py     # Test suite for registration, cancellation & retries
│
├── requirements.txt             # Project dependencies
├── run_all.py                   # Master CLI runner
└── README.md                    # Project documentation
```

---

## ⚡ Quick Start Guide

### 1. Prerequisites & Installation

Clone the repository and install the minimal dependencies:

```bash
git clone https://github.com/RohanShah31/gdg-backend-wildcard.git
cd gdg-backend-wildcard
pip install -r requirements.txt
```

### 2. Run All Automated Tests
```bash
python run_all.py test
```
*Executes all 11 unit and integration tests across both tasks.*

---

## 📡 Task 1: Campus Sensor Monitoring and Alerts

### Architecture & 10,000 Readings/Sec Ingestion
A standard relational database fails if you make 10,000 individual HTTP requests with synchronous single-row SQL `INSERT` statements. 

To solve this cleanly without over-engineering:
1. **In-Memory Hot Path:** When a batch arrives at `/api/readings/batch`, the endpoint validates the data and evaluates active alert rules in RAM in sub-millisecond time.
2. **Write-Behind Queue:** Telemetry records are enqueued into an in-memory `asyncio.Queue`. The HTTP endpoint returns HTTP 200 within tens of milliseconds.
3. **Bulk Commits:** A background worker drains the queue and writes records in bulk transactions using SQLite's `executemany` with Write-Ahead Logging (WAL).
4. **Alert Storm Cooldown:** To prevent 10,000 duplicate alert rows when a sensor spikes during a burst, an in-memory cooldown cache deduplicates active alerts per sensor.

```
Incoming Burst (10,000 readings) ──► FastAPI Endpoint ──► In-Memory Rule Engine (RAM)
                                              │                       │
                                              ▼ (Non-blocking)        ▼ (Immediate)
                                         Ingest Queue            Insert Alert Event
                                              │
                                              ▼
                                   Background SQLite WAL
```

### Sliding Window Rule Evaluation
- **Single-Reading Rules:** Compared instantly against incoming metrics (`>`, `>=`, `<`, `<=`, `==`, `!=`). E.g., `temperature > 35.0` or `occupancy > 120`.
- **Time-Window Rules:** Maintains an in-memory `collections.deque` per `(sensor_id, type)` storing `(timestamp, value)`. Readings older than the time window (e.g., 10 minutes) are pruned in $O(1)$ amortized time. Computes rolling `avg`, `max`, `min`, or `count`.

### Verified Load Test Results

Tested on a local machine using `python run_all.py loadtest`:

| Metric | Measured Result | Target Benchmark | Verdict |
| :--- | :--- | :--- | :--- |
| **Total Wall-Clock Time** | **0.9332 s** | `< 1.000 s` | ✅ **TARGET ACHIEVED** |
| **Ingestion Throughput** | **10,715.3 readings/sec** | `10,000 readings/sec` | ✅ **EXCEEDED** |
| **Request Success Rate** | **10/10 (100%)** | `100%` | ✅ **PERFECT** |
| **Median (p50) Latency** | **370.02 ms** | `< 500 ms` | ✅ **EXCELLENT** |
| **p95 Latency** | **926.49 ms** | `< 1000 ms` | ✅ **PASSED** |

Full load test report is available in [`task1_sensor_monitoring/LOAD_TEST_REPORT.md`](task1_sensor_monitoring/LOAD_TEST_REPORT.md).

### Telemetry Simulator
Run continuous telemetry generation simulating sensors across campus (library, server rooms, labs, auditoriums):
```bash
# Terminal 1: Start backend
python run_all.py task1

# Terminal 2: Run simulator
python run_all.py simulator
```

---

## 🎫 Task 2: Workshop Registration and Notifications

### The Transactional Outbox Pattern
Calling external notification APIs directly inside an HTTP handler introduces the **Dual-Write Problem**: if the third-party notification service times out or errors, the user gets a 500 error even though the registration was saved in the database.

Our solution implements the **Transactional Outbox Pattern**:
- When a user registers (`POST /api/workshops/{id}/register`), the `registrations` record and the `notifications` job are committed in the **SAME atomic database transaction**.
- The API returns HTTP 201 immediately in under 20ms without waiting for downstream HTTP calls.
- If registration fails (e.g. member already registered or workshop full), transaction rolls back—**guaranteeing failed registrations never produce confirmations**.
- If the server crashes or reboots, the notification jobs remain safe on disk in the SQLite database.

```
Member Register ──► [ SQLite Atomic Transaction ]
                           ├── 1. INSERT registration
                           └── 2. INSERT notification (Outbox)
                                        │
                       Immediate 201 OK ┼──► Client gets confirmation (<20ms)
                                        ▼
                               [ Outbox Worker ]
                                        │ (Concurrent Dispatch)
                                        ▼
                         [ Mock Notification Service ]
```

### Handling Failures, Restarts & Duplicate Delivery

1. **Independent Concurrency:** The outbox worker dispatches notifications concurrently using an `asyncio.Semaphore`. If Member A's delivery fails or times out, it backs off independently without blocking Member B.
2. **Exponential Backoff with Jitter:** When the downstream service fails, retries back off exponentially:
   $$\text{delay} = \min(30.0, 1.0 \times 2^{(\text{attempts}-1)}) + \text{jitter}$$
3. **Idempotency Key (`X-Idempotency-Key`):** Prevents duplicate deliveries if a network packet drops after downstream receipt.
4. **Crash Recovery:** When the backend restarts, the worker automatically queries the database for pending or retrying jobs and resumes delivery.
5. **Workshop Cancellation:** Cancelling a workshop updates status to `CANCELLED` and atomically enqueues cancellation notifications for all registered members.

### Interactive Resilience Demonstration
Demonstrates the complete fault-tolerance lifecycle in one command:
```bash
# Terminal 1: Start mock service
python run_all.py mock2

# Terminal 2: Start workshop service
python run_all.py task2

# Terminal 3: Run resilience demo
python run_all.py demo2
```
*Demonstrates instant registration, duplicate rejection (409 Conflict), downstream outage handling (HTTP 503), retry scheduling, recovery delivery, and cancellation broadcast.*

---

## 🧪 Automated Test Suite

Run all tests across both tasks:

```bash
python run_all.py test
```

### Test Coverage Summary:
- **Task 1 (`test_sensors.py`):**
  - ✅ Health check & bootstrapping
  - ✅ Single reading ingestion & immediate threshold alert triggering
  - ✅ High-throughput batch ingestion
  - ✅ Dynamic rule creation, editing, and enabling/disabling
  - ✅ 10-minute sliding time-window rolling average calculation
  - ✅ Alert acknowledgment & resolution lifecycle
- **Task 2 (`test_workshop.py`):**
  - ✅ Workshop creation & capacity enforcement
  - ✅ Atomic member registration & immediate outbox entry
  - ✅ Duplicate email registration rejection (HTTP 409 Conflict)
  - ✅ Workshop cancellation broadcast to all confirmed attendees

---

## 🎤 Interview Explanation Guide

Use this concise narrative to explain the project during your evaluation:

### 1. High-Level Summary
> *"For this challenge, I completed both Task 1 and Task 2 using Python and FastAPI with an embedded SQLite database configured in WAL mode. I avoided heavy external brokers like Kafka or Redis to keep the architecture lightweight, transparent, and easy to run locally while adhering to production-grade distributed systems patterns."*

### 2. Task 1: How did you achieve 10,000 readings in 1 second?
> *"In a high-throughput monitoring backend, synchronous disk writes per reading create an I/O bottleneck. I implemented a two-tier architecture:*
> *First, the **In-Memory Tier**: incoming readings are validated and immediately evaluated against rules in RAM. Single-reading rules take sub-microsecond time, and time-window rules use circular sliding deques (`collections.deque`) to prune expired readings and compute rolling averages in O(1) amortized time.*
> *Second, the **Decoupled Persistence Tier**: readings are enqueued into an in-memory queue, and a background worker commits them in bulk chunks using SQLite's `executemany` with Write-Ahead Logging (WAL). This allowed us to ingest and evaluate 10,000 readings in 0.93 seconds with 100% success rate."*

### 3. Task 2: How did you guarantee reliable notifications across failures and restarts?
> *"I implemented the **Transactional Outbox Pattern**.*
> *Instead of calling the notification service synchronously inside the registration request—which is slow and risks partial failure—we insert both the registration and the notification job in the same atomic database transaction. The user receives an immediate response in under 20ms.*
> *A background outbox worker polls for pending jobs and dispatches them concurrently. If the mock service goes down, the worker retries with **exponential backoff and jitter**, ensuring Member A's retry never blocks Member B. Each message carries an **idempotency key** to prevent duplicate deliveries, and because all jobs are stored on disk in the database, if the backend crashes or reboots, the worker resumes right where it left off upon startup."*

---

## 📜 License
This project is open-source and available under the [MIT License](LICENSE).
