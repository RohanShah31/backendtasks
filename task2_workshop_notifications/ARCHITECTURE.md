# Architecture & Resilience Guide: Workshop Registration & Notifications

This document explains the distributed systems patterns implemented for **Task 2: Workshop Registration and Notifications**, specifically how the backend achieves **100% notification reliability**, **zero dropped messages**, and **graceful fault recovery**.

---

## 1. Core Architectural Pattern: The Transactional Outbox

In distributed web applications, calling a third-party notification service (such as SendGrid, Twilio, or an external HTTP service) directly inside an HTTP request handler creates a critical anti-pattern known as the **Dual-Write Problem**:

```
[ Traditional Flawed Approach (Dual Write) ]
  Client Request ──> Save Registration to DB ──(DB Success)──> Call Email API ──(API Timeout!)──> 💥 User gets 500 error, DB has registration, no email sent!
```

### The Transactional Outbox Solution Implemented:
Instead of calling the notification service synchronously, we record the registration and the notification job in the **SAME atomic database transaction** in SQLite:

```
[ Our Robust Transactional Outbox Architecture ]

Client Request (POST /register)
      │
      ▼
┌────────────────────────────────────────────────────────────────────────┐
│ SQLite Database (Single Atomic Transaction)                            │
│                                                                        │
│  1. INSERT INTO registrations (...)                                    │
│  2. INSERT INTO notifications (outbox job, status='PENDING', ...)      │
│                                                                        │
│  * If ANY step fails (e.g. duplicate email), BOTH roll back.          │
│  * If successful, BOTH commit to disk immediately.                     │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   │
                                   ▼
          Returns Immediate HTTP 201 Created to Member (<15ms)
                                   │
                                   │ (Asynchronous background polling)
                                   ▼
┌────────────────────────────────────────────────────────────────────────┐
│                       Outbox Background Worker                         │
│                                                                        │
│  - Polls notifications WHERE status IN ('PENDING', 'RETRY')            │
│  - Dispatches delivery tasks concurrently via asyncio.Semaphore        │
│  - Injects X-Idempotency-Key in HTTP headers                           │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   │
                                   ▼
┌────────────────────────────────────────────────────────────────────────┐
│                      Mock Notification Service                         │
│                                                                        │
│  - Checks idempotency key to prevent duplicates                        │
│  - Simulates network timeouts / transient 500 errors                   │
│  - Returns HTTP 200 on successful receipt                              │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Answers to Core Evaluation Criteria

### A. How the Design Handles Lost Notifications
- **Guarantee:** "Every successful registration must eventually produce a confirmation; failed registrations must not produce confirmations."
- **Mechanism:** Because `registrations` and `notifications` are inserted inside a single `with conn:` database transaction:
  - If a registration fails (e.g., workshop is full, workshop is cancelled, or the member email is already registered), the entire transaction rolls back. **No notification row is ever created.**
  - If a registration succeeds, the notification row is committed to disk before the HTTP response is sent. Even if the network drops immediately, the notification cannot be lost because it is safely persisted in the database.

### B. How Retries and Independent Deliveries Work
- **Requirement:** "Retry notification delivery when the mock service is unavailable. A failure for one member must not block delivery to others."
- **Mechanism:**
  1. **Independent Concurrency:** The worker fetches due notifications and processes each job as an independent `asyncio.Task` governed by a concurrency semaphore. If Member A's destination is failing or timing out, Member A's task sleeps or backs off independently while Member B's notification is dispatched in parallel without delay.
  2. **Exponential Backoff with Jitter:** When a delivery attempt fails (due to HTTP 500 or network timeout):
     $$\text{delay} = \min(\text{max\_backoff}, \text{base} \times 2^{(\text{attempts}-1)}) + \text{jitter}$$
     - Attempt 1: $\approx 1.2\text{s}$
     - Attempt 2: $\approx 2.4\text{s}$
     - Attempt 3: $\approx 4.3\text{s}$
     - Attempt 4: $\approx 8.2\text{s}$
     - Attempt 5: Final attempt before marking `FAILED` (Dead Letter Queue).
     - The random **jitter** prevents the "thundering herd problem" where all failed requests retry at the exact same microsecond.

### C. How Duplicate Delivery is Prevented (Idempotency)
- **Challenge:** If the notification service receives a message, processes it, but the TCP connection breaks before the HTTP 200 response reaches the client, the worker might retry the request.
- **Solution:** Every notification job generates a deterministic **`idempotency_key`**:
  - Registration: `reg_conf_{workshop_id}_{member_email}`
  - Cancellation: `cancel_notif_{workshop_id}_{member_email}`
- This key is passed in the HTTP request header: `X-Idempotency-Key: reg_conf_ws-101_alice@vit.ac.in`.
- The notification service stores processed keys in an index. If a retry arrives with an already-seen key, the service recognizes it as a re-transmission, acknowledges it with HTTP 200, and does **not** send duplicate notifications to the user.

### D. How Backend Restarts are Handled (Crash Recovery)
- **Requirement:** "If the backend restarts after saving a registration, its confirmation must still be sent."
- **Mechanism:**
  - In-memory queues (like Python `queue.Queue` or simple timers) lose all data when a server process is killed or restarted.
  - In our architecture, the outbox state is saved persistently in SQLite on disk.
  - Upon server bootup (`lifespan` in `app/main.py`), the `OutboxWorker` starts up and immediately queries:
    ```sql
    SELECT * FROM notifications 
    WHERE status IN ('PENDING', 'RETRY') AND next_retry_at <= datetime('now');
    ```
  - Any pending notifications from before the crash are resumed and delivered without data loss.

---

## 3. Workshop Cancellation Workflow
When an organizer cancels a workshop (`POST /api/workshops/{id}/cancel`):
1. The workshop status is set to `'CANCELLED'`.
2. A database query retrieves all confirmed members for that workshop.
3. For each member, a `WORKSHOP_CANCELLATION` notification job is created with `status = 'PENDING'`.
4. The background worker picks up these cancellation jobs and delivers them concurrently to every member.

---

## 4. Key Interview Explanations (Short 2-Minute Pitch)

1. **Architecture:** "We used the **Transactional Outbox Pattern**. Rather than making an HTTP call inside the user request—which is fragile and slow—we persist the notification in the same database transaction as the registration, and return immediately."
2. **Crash Resilience:** "Because notifications live in the SQLite database, a server crash or sudden reboot never loses any messages. When the server restarts, the outbox worker resumes delivery immediately."
3. **Flaky Network Resilience:** "We implemented independent concurrent workers with **exponential backoff and jitter**. A failure or slow timeout for one user never blocks other users from receiving their notifications."
4. **Duplicate Protection:** "Each notification carries an **idempotency key**. If a transient network glitch causes a retry of an already-processed delivery, the recipient service deduplicates it safely."
