# Architecture & Design Deep Dive

This document expands on the root [README.md](../README.md) with full rationale, every failure-scenario walkthrough, and the testing/validation log. Read the README first for the overview; read this for the "why" behind each decision and the evidence behind each claim.

## Table of Contents

- [Task Model](#task-model)
- [Scheduling](#scheduling)
- [Concurrency Model](#concurrency-model)
- [Persistence](#persistence)
- [Crash Recovery](#crash-recovery)
- [Retry and Exponential Backoff](#retry-and-exponential-backoff)
- [Concurrency Limiting with Semaphore](#concurrency-limiting-with-semaphore)
- [Task Handler Abstraction](#task-handler-abstraction)
- [Metrics and Observability](#metrics-and-observability)
- [SQLite Concurrency](#sqlite-concurrency)
- [Failure Scenarios](#failure-scenarios)
- [Testing and Validation](#testing-and-validation)
- [Design Decisions](#design-decisions)
- [Delivery Semantics](#delivery-semantics)
- [Limitations](#limitations)
- [What I Learned](#what-i-learned)
- [Future Improvements](#future-improvements)

---

## Task Model

```text
Task
 ├── id
 ├── name
 ├── taskType
 ├── payload
 ├── weight
 ├── state
 ├── enqueueTime
 ├── retryCount
 ├── nextRetryAt
 └── CompletableFuture
```

The `CompletableFuture` lets the caller receive the eventual result of asynchronous execution without blocking a scheduler worker thread.

---

## Scheduling

### Weighted priority

Each task has a `weight`; higher weight means higher scheduling priority. The `PriorityBlockingQueue` uses a custom `TaskComparator` based on **effective priority**:

```text
Effective Priority = Weight + Aging Bonus
```

Example:

```text
Task A: weight 10, waited 2s → aging bonus 2 → effective priority 12
Task B: weight 5,  waited 2s → aging bonus 2 → effective priority 7
```

Task A runs first — but priority isn't fixed permanently, which is the point of aging.

### Starvation avoidance

A pure priority scheduler can starve low-priority tasks indefinitely if high-priority work keeps arriving. Aging counters this: a task gains effective priority the longer it waits —

```text
agingBonus = waitingTime / 1000
```

— and the queue is periodically refreshed so the comparator re-evaluates tasks against their updated effective priority. This is intentionally simple; it demonstrates the core principle without building a full fair-scheduling framework.

### Tie-breaking

When effective priority ties, ordering falls back to: (1) higher effective priority, (2) older enqueue time, (3) lower task ID — giving predictable behavior instead of relying on `PriorityBlockingQueue`'s undefined tie ordering.

---

## Concurrency Model

```text
ExecutorService → Fixed Thread Pool → 4 Worker Threads
```

Each worker loops: take a task from the queue → execute it → update persistent state → complete or schedule a retry → repeat. `PriorityBlockingQueue`'s blocking `take()` means workers wait for work without hand-rolled synchronization.

---

## Persistence

Task state is persisted to SQLite via JDBC:

```text
tasks
├── id, name, task_type, payload, weight
├── state
├── enqueue_time
├── retry_count
└── next_retry_at
```

Without persistence, a crash means the in-memory queue — and all knowledge of unfinished work — disappears with the JVM. With it, task state survives independently of the process, so a restart can recover what was left incomplete.

---

## Crash Recovery

On startup, the repository queries for tasks in `PENDING`, `RUNNING`, or `RETRYING` — anything not completed before shutdown or crash:

- **PENDING** → placed back into the queue as-is.
- **RUNNING** → a task that was `RUNNING` when the process died cannot still be executing in a terminated JVM, so it's re-queued and executed again. This is the specific reason the scheduler provides *at-least-once*, not exactly-once, semantics (see [Delivery Semantics](#delivery-semantics)).
- **RETRYING** → the persisted `nextRetryAt` timestamp is read back, the remaining delay is computed, and the retry is rescheduled — so backoff state survives a restart instead of resetting.

---

## Retry and Exponential Backoff

Failed tasks aren't retried immediately. Backoff grows exponentially:

```text
Retry 1 → 1s
Retry 2 → 2s
Retry 3 → 4s

delay = 1000 × 2^(retryCount - 1)
```

After the max retry count, the task moves to `DEAD` and its `CompletableFuture` completes exceptionally.

**Why `ScheduledExecutorService` instead of `Thread.sleep()` in a worker?** A worker blocked on `sleep()` during a retry delay is a worker doing nothing useful — it can't pick up other queued work. A `ScheduledExecutorService` handles the delay independently: it schedules the task's return to the queue for a future time without occupying a worker thread in the meantime.

```text
Worker: task fails → hands off to Retry Scheduler
Retry Scheduler: waits out the backoff delay independently
                 → returns task to the priority queue
Worker pool: free to process other tasks in the meantime
```

---

## Concurrency Limiting with Semaphore

4 worker threads exist, but handler execution is gated by `Semaphore(2)` — at most 2 handlers run concurrently, regardless of how many workers are free:

```text
4 Workers → Semaphore(2) → Handler
```

This is **concurrency limiting**, not rate limiting: it caps how many operations run *at the same time*, with no notion of a time window or operations-per-second. A true rate limiter (e.g. token bucket) would be a different, complementary mechanism — see [Future Improvements](#future-improvements).

---

## Task Handler Abstraction

```java
public interface TaskHandler {
    String execute(String payload) throws Exception;
}
```

The scheduler doesn't know how any given task type is implemented — it only knows a task type maps to a handler via `TaskHandlerRegistry` (`SEND_EMAIL` → `SendEmailHandler`, `FAIL_TASK` → `FailingTaskHandler`). Adding a new task type (e.g. `GENERATE_REPORT`, `PROCESS_IMAGE`) means registering a new handler, not touching scheduling logic.

---

## Metrics and Observability

Thread-safe counters via `AtomicInteger`, since multiple workers update them concurrently:

```text
Submitted · Completed · Retried · Dead · Currently Running
```

---

## SQLite Concurrency

Initial stress testing with concurrent writers produced:

```text
SQLITE_BUSY: database is locked
```

Fix applied:

```sql
PRAGMA journal_mode=WAL;
PRAGMA busy_timeout=5000;
```

**WAL** (Write-Ahead Logging) improves SQLite's behavior under concurrent readers/writers. **Busy timeout** makes SQLite wait for a lock to clear for a bounded period instead of failing immediately. After this change, the 500-task stress test completed without the earlier lock failure.

---

## Failure Scenarios

**Scenario 1 — success:** `PENDING → RUNNING → COMPLETED`, future completes with the handler's result.

**Scenario 2 — transient failure, recovers:**
```text
PENDING → RUNNING → RETRYING → (backoff) → PENDING → RUNNING → COMPLETED
```

**Scenario 3 — repeated failure, exhausts retries:**
```text
RUNNING → RETRYING → RUNNING → RETRYING → RUNNING → RETRYING → RUNNING → DEAD
```
Future completes exceptionally once `DEAD` is reached.

**Scenario 4 — process crashes mid-task:**
```text
Task state = RUNNING → process crashes → JVM terminates
→ task remains RUNNING in SQLite → restart
→ repository finds unfinished task → task recovered and re-executed
```
This is the concrete reason task state is persisted independently of the in-memory queue.

---

## Testing and Validation

Tested incrementally rather than relying on "it started without an exception."

**Basic execution** — submitted tasks are consumed by workers and complete successfully.

**Weighted scheduling** — higher-weight tasks receive higher effective priority and run first.

**Starvation avoidance** — waiting time visibly raises effective priority, letting older low-priority tasks become competitive against newer high-priority ones.

**Retry and DEAD state** — a dedicated `FailingTaskHandler` forces deterministic failures to exercise the retry path end to end:

```text
HANDLER STARTED: Retry-Test
TASK FAILED: Retry-Test | retry 1
RETRYING TASK: Retry-Test | retry 1

HANDLER STARTED: Retry-Test
TASK FAILED: Retry-Test | retry 2
RETRYING TASK: Retry-Test | retry 2

HANDLER STARTED: Retry-Test
TASK FAILED: Retry-Test | retry 3
RETRYING TASK: Retry-Test | retry 3

HANDLER STARTED: Retry-Test
TASK DEAD: Retry-Test
```

This validated retry scheduling, multiple attempts, retry persistence, the exponential backoff progression, max-retry handling, and the `DEAD` transition.

**Crash recovery mid-retry** — a task was allowed to enter `RETRYING` state, then the process was killed. On restart:

```text
RECOVERED: Retry-Test (state=RETRYING)
RECOVERED RETRY READY: Retry-Test
```

This validated that retry state (not just plain queued state) survives a process restart.

**Semaphore concurrency limiting** — with 4 workers and `Semaphore(2)`, only two handler executions ran concurrently at any point, confirmed by interleaved `HANDLER STARTED` logs never showing more than two in-flight before a completion freed a permit.

**Stress test (500 tasks)** —

```text
===== METRICS =====
Submitted: 500
Completed: 500
Retried: 0
Dead: 0
Currently Running: 0
===================
```

Process exited cleanly (code `0`). This is a validation/stress test, not a formal throughput or latency benchmark — no performance numbers are claimed from it. Its main purpose was surfacing the `SQLITE_BUSY` issue under concurrent load and confirming the WAL/busy-timeout fix resolved it on rerun.

---

## Design Decisions

**Why `PriorityBlockingQueue`?** A plain queue gives FIFO ordering but no natural way to express weighted scheduling. This gives thread-safe access, blocking `take()`, and priority ordering in one structure.

**Why `ExecutorService`?** Spinning up a thread per task doesn't scale. A fixed pool of reusable workers continuously consumes from the queue instead.

**Why `CompletableFuture`?** The scheduler runs tasks asynchronously, but callers still need the eventual result — this gives them a handle without blocking a worker thread on their behalf.

**Why SQLite?** Real persistence and recovery, without the project becoming a database-infrastructure exercise. SQL semantics, JDBC, transactional behavior, no server to run — used to demonstrate persistence and recovery, not as a production-grade distributed store.

**Why `ScheduledExecutorService` for retries?** Keeps backoff delays off the worker pool entirely (see [Retry and Exponential Backoff](#retry-and-exponential-backoff)).

**Why `Semaphore`, separate from the thread pool?** `ExecutorService` controls how many workers exist; `Semaphore` controls how many are allowed to touch a specific resource/handler at once. Different concerns — a thread pool can't express "downstream resource X only tolerates 2 concurrent calls" on its own.

**Why exponential backoff?** Immediate retries can hammer a dependency that's only temporarily unavailable. Growing delays (1s → 2s → 4s) reduce repeated pressure and give transient failures room to resolve.

**Why WAL + busy timeout?** Directly in response to an observed failure (`SQLITE_BUSY`) during the first 500-task stress test, not a preemptive guess — see [SQLite Concurrency](#sqlite-concurrency).

---

## Delivery Semantics

This scheduler provides **at-least-once** execution, not exactly-once:

```text
1. Handler performs an external side effect
2. Process crashes
3. Database still shows RUNNING
4. Scheduler restarts
5. Task executes again
```

The external operation can therefore run more than once. For real side-effecting handlers (e.g. actually sending an email or charging a payment), exactly-once would require idempotency keys or transactional coordination with the external system — not implemented here, and called out explicitly rather than glossed over.

---

## Limitations

Deliberate scope boundaries, not hidden gaps:

- Single JVM/process — no distributed workers, no leader election, no distributed locking, no external message broker
- SQLite is local storage, not a multi-process/multi-node datastore
- No exactly-once execution guarantee
- No production email provider integration (handler is a stub)
- No REST API, no authentication/authorization
- No centralized monitoring system (metrics are printed, not exported)

---

## What I Learned

Concurrency and reliability concepts explored hands-on: thread-safe producer/consumer design, priority scheduling with aging/starvation-avoidance, `ExecutorService` and `ScheduledExecutorService`, `PriorityBlockingQueue`, `CompletableFuture`, `Semaphore`, persistent state machines, crash recovery, retry semantics and exponential backoff, at-least-once execution trade-offs, JDBC, SQLite locking behavior and WAL mode, thread-safe metrics, and failure-oriented (not just happy-path) testing.

The clearest practical lesson: persistence and concurrency together introduce failure modes invisible in a simple in-memory version. The first 500-task stress test surfaced a real `SQLITE_BUSY` failure — the fix was applied and verified by rerunning under the same load, rather than assuming the problem away.

---

## Future Improvements

- REST API for task submission
- Task cancellation
- Configurable worker count and retry policy
- Persistent task result storage
- Dead-letter task inspection/replay tooling
- A true time-based rate limiter (token bucket) layered alongside the existing semaphore-based concurrency limiting, for cases with an actual external rate limit to respect
- Graceful shutdown with in-flight task draining
- Idempotency support for external side effects
- Migration from SQLite to a server-based database for multi-process deployments

These are intentionally out of scope for the current implementation.
