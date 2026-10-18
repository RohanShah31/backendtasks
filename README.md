# GDG Tech Wildcard Entry — Backend Development Suite

> **Complete solution for both Backend Challenge Tasks:**
> 1. **Task 1: Campus Sensor Monitoring & Alerts** (High-throughput batch ingestion of 10,000+ readings/sec, in-memory sliding time-window rule evaluation, telemetry simulator, and burst load-test report).
> 2. **Task 2: Workshop Registration & Notifications** (Transactional Outbox pattern, non-blocking registration, exponential backoff retries, duplicate prevention with idempotency keys, crash recovery, and resilience demo).

---

## 📑 Table of Contents

- [Tech Stack Selection](#-tech-stack-selection)
- [Project Architecture & Directory Layout](#-project-architecture--directory-layout)
- [Quick Start Guide](#-quick-start-guide)
- [Task 1: Campus Sensor Monitoring and Alerts](#-task-1-campus-sensor-monitoring-and-alerts)
  - [Architecture & 10,000 Readings/Sec Ingestion](#architecture--10000-readingssec-ingestion)
  - [Rule Engine (Single-Reading & Time-Window)](#rule-engine-single-reading--time-window)
  - [Running the Simulator & Load Test](#running-the-simulator--load-test)
- [Task 2: Workshop Registration and Notifications](#-task-2-workshop-registration-and-notifications)
  - [The Transactional Outbox Pattern](#the-transactional-outbox-pattern)
  - [Retry Logic, Idempotency & Backend Restarts](#retry-logic-idempotency--backend-restarts)
  - [Running the Resilience Demo](#running-the-resilience-demo)
- [Running Automated Tests](#-running-automated-tests)
- [Interview Explanation Guide (How to Explain the Entire Project in 3 Minutes)](#-interview-explanation-guide)

---

## 🛠 Tech Stack Selection

To keep the solution **robust, performant, and easy to explain without convoluted infrastructure**, we chose:

- **Language:** Python 3.13
- **Web Framework:** **FastAPI** (Asynchronous endpoints, native OpenAPI/Swagger docs, high throughput).
- **Data Validation:** **Pydantic v2** (Fast parsing and strict schema enforcement).
- **Database Engine:** **SQLite in WAL Mode** (`PRAGMA journal_mode = WAL`, `PRAGMA synchronous = NORMAL`). Provides zero-setup local persistence with up to 50,000+ bulk writes/sec and full ACID safety without requiring separate external database servers.
- **HTTP Client:** **HTTPX** (Asynchronous connection pooling for workers).
- **Testing:** **Pytest** + **FastAPI TestClient**.

---

## 📁 Project Architecture & Directory Layout

```
taskgdg/
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
└── README.md                    # This documentation
```

---

## 🚀 Quick Start Guide

### 1. Prerequisites & Installation
Ensure Python 3.10+ is installed. Then install requirements:

```bash
pip install -r requirements.txt
```

### 2. Running All Tests (Task 1 & Task 2)
```bash
python run_all.py test
```
*Executes all 11 unit and integration tests across both tasks.*

---

## 📡 Task 1: Campus Sensor Monitoring and Alerts

### Architecture & 10,000 Readings/Sec Ingestion
A standard relational database chokes if you make 10,000 individual HTTP requests with synchronous single-row SQL `INSERT`s. To solve this cleanly:

1. **In-Memory Hot Path:** When a batch arrives at `/api/readings/batch`, the endpoint validates the data and evaluates active alert rules in RAM in sub-millisecond time.
2. **Write-Behind Queue:** Telemetry records are enqueued into an in-memory `asyncio.Queue`. The HTTP endpoint returns HTTP 200 within tens of milliseconds.
3. **Bulk Commits:** A background worker drains the queue and writes records in bulk transactions using SQLite's `executemany` with Write-Ahead Logging (WAL).
4. **Alert Storm Cooldown:** To prevent 10,000 duplicate alert rows when a sensor spikes during a burst, an in-memory cooldown cache deduplicates active alerts per sensor.

```
Incoming Burst (10k readings) ──> FastAPI Endpoint ──> In-Memory Rule Engine (RAM)
                                          │                      │
                                          ▼ (Non-blocking)       ▼ (Immediate)
                                     Ingest Queue           Insert Alert Event
                                          │
                                          ▼
                               Background SQLite WAL
```

### Rule Engine (Single-Reading & Time-Window)
- **Single-Reading Rules:** Compared instantly against incoming metrics (`>`, `>=`, `<`, `<=`, `==`, `!=`). E.g., `temperature > 35.0` or `occupancy > 120`.
- **Time-Window Rules:** Maintains an in-memory `collections.deque` per `(sensor_id, type)` storing `(timestamp, value)`. Readings older than the time window (e.g., 10 minutes) are pruned in $O(1)$ amortized time. Computes rolling `avg`, `max`, `min`, or `count`.

### Running the Simulator & Load Test

1. **Start Task 1 Service (Port 8000):**
   ```bash
   python run_all.py task1
   ```
   *Interactive Swagger Documentation available at `http://127.0.0.1:8000/docs`*

2. **Run the Sensor Simulator (in a new terminal):**
   ```bash
   python run_all.py simulator
   ```
   *Simulates continuous sensor data from library floors, server rooms, auditoriums, and labs with realistic anomaly spikes.*

3. **Run the 10,000 Readings/Sec Burst Load Test:**
   ```bash
   python run_all.py loadtest
   ```
   *Sends 10,000 readings across concurrent workers. Verifies completion in under 1.0 second and generates [LOAD_TEST_REPORT.md](file:///c:/Users/hp/Desktop/taskgdg/task1_sensor_monitoring/LOAD_TEST_REPORT.md).*

---

## 🎫 Task 2: Workshop Registration and Notifications

### The Transactional Outbox Pattern
Calling external notification APIs directly inside an HTTP handler creates the **Dual-Write Problem** (if the email service fails, the user gets an error even though they were registered, or vice-versa).

Our solution uses the **Transactional Outbox Pattern**:
- When a user registers (`POST /api/workshops/{id}/register`), the `registrations` record and the `notifications` job are committed in the **SAME atomic database transaction**.
- The API returns HTTP 201 immediately in under 20ms without waiting for downstream HTTP calls.
- If registration fails (e.g. member already registered or workshop full), transaction rolls back—**guaranteeing failed registrations never produce confirmations**.
- If the server crashes or reboots, the notification jobs remain safe in the SQLite database on disk.

```
Member Register ──> [ SQLite Transaction ]
                          ├── 1. INSERT registration
                          └── 2. INSERT notification (Outbox)
                                       │
                      Immediate 201 OK ┼──> Client gets confirmation (<20ms)
                                       ▼
                              [ Outbox Worker ]
                                       │ (Concurrent Dispatch)
                                       ▼
                        [ Mock Notification Service ]
```

### Retry Logic, Idempotency & Backend Restarts
1. **Independent Concurrency:** The outbox worker dispatches notifications concurrently using an `asyncio.Semaphore`. If Member A's delivery fails or times out, it backs off independently without blocking Member B.
2. **Exponential Backoff with Jitter:** When the downstream service fails, retries back off exponentially:
   $$\text{delay} = \min(30.0, 1.0 \times 2^{(\text{attempts}-1)}) + \text{jitter}$$
3. **Idempotency Key (`X-Idempotency-Key`):** Prevents duplicate deliveries if a network packet drops after downstream receipt.
4. **Crash Recovery:** When the backend restarts, the worker automatically queries the database for pending or retrying jobs and resumes delivery.
5. **Workshop Cancellation:** Cancelling a workshop updates status to `CANCELLED` and atomically enqueues cancellation notifications for all registered members.

### Running the Resilience Demo

1. **Start Mock Notification Service (Port 8002):**
   ```bash
   python run_all.py mock2
   ```

2. **Start Workshop Registration Service (Port 8001):**
   ```bash
   python run_all.py task2
   ```

3. **Run the Interactive Demonstration Script:**
   ```bash
   python run_all.py demo2
   ```
   *Demonstrates the full lifecycle: workshop creation, instant registration, duplicate rejection, simulated downstream outage, retry scheduling, automatic recovery, and cancellation broadcasting.*

---

## 🧪 Running Automated Tests

All tests can be executed with a single command:

```bash
python run_all.py test
```

### What is tested:
- **Task 1 (`test_sensors.py`):**
  - Health check & API bootstrapping
  - Single reading ingestion & immediate alert triggering
  - Batch ingestion
  - Dynamic rule creation, updating, and toggling
  - 10-minute sliding time-window rolling average calculation
  - Alert acknowledgment & resolution lifecycle
- **Task 2 (`test_workshop.py`):**
  - Workshop creation & capacity enforcement
  - Atomic member registration & immediate outbox entry
  - Duplicate email registration rejection (HTTP 409 Conflict)
  - Workshop cancellation broadcast to all confirmed attendees

---

## 🎤 Interview Explanation Guide

Use this concise narrative to explain the project during your evaluation:

### 1. Project High-Level Summary
> *"For this challenge, I completed both Task 1 and Task 2 using Python and FastAPI with an embedded SQLite WAL database. I intentionally kept the design clean, decoupled, and modular so it handles high throughput and distributed resilience patterns without introducing unnecessary external infrastructure like Redis or Kafka."*

### 2. Task 1: How did you achieve 10,000 readings in 1 second?
> *"In a high-throughput monitoring backend, synchronous disk writes per reading create an I/O bottleneck. I implemented a two-tier architecture:*
> *First, the **In-Memory Tier**: incoming readings are validated and immediately evaluated against rules in RAM. Single-reading rules take sub-microsecond time, and time-window rules use circular sliding deques (`collections.deque`) to prune expired readings and compute rolling averages in O(1) amortized time.*
> *Second, the **Decoupled Persistence Tier**: readings are enqueued into an in-memory queue, and a background worker commits them in bulk chunks using SQLite's `executemany` with Write-Ahead Logging (WAL). This allowed us to ingest and evaluate 10,000 readings in 0.93 seconds with 100% success rate."*

### 3. Task 2: How did you guarantee reliable notifications across failures and restarts?
> *"I implemented the **Transactional Outbox Pattern**.*
> *Instead of calling the notification service synchronously inside the registration request—which is slow and risks partial failure—we insert both the registration and the notification job in the same atomic database transaction. The user receives an immediate response in under 20ms.*
> *A background outbox worker polls for pending jobs and dispatches them concurrently. If the downstream mock service fails or times out, the worker applies **exponential backoff with random jitter**, and because each notification is dispatched independently, one member's failure never blocks others.*
> *Each job carries a unique **idempotency key** to prevent duplicate deliveries, and because all jobs are stored on disk in the database, if the backend crashes or reboots, the worker resumes right where it left off upon startup."*
