# kafka-clickstream-pipeline

A Kafka-based event streaming pipeline that ingests simulated user clickstream events, keyed by user ID for guaranteed per-user ordering, and performs real-time sessionization — grouping each user's activity into sessions with automatic closure after a period of inactivity.

---

## What this project demonstrates

This is the third in a series of three job-processing systems built at increasing sophistication, each isolating a different message-broker tradeoff:

1. **[postgres-job-queue](https://github.com/SiddhantGupta3112/postgres-job-queue)** — job dispatch using only database primitives, no external broker.
2. **[event-driven-task-queue](https://github.com/SiddhantGupta3112/event-driven-task-queue)** — introduces Redis Streams for delivery guarantees and a self-scaling worker pool.
3. **This project** — introduces Kafka specifically to demonstrate **partition-based, per-key ordering guarantees** and **automatic consumer-group rebalancing**, neither of which Redis Streams provides in the same form.

**Sessionization is the workload, not the point of the project.** The point is correct Kafka usage: partition key design, consumer-group semantics, manual offset management for at-least-once correctness, and idempotent-safe writes that survive partition rebalances. Sessionization was chosen as the demonstration workload specifically because it is a real pattern (used in analytics, fraud detection, recommendation systems) that only produces correct results if the ordering guarantee actually holds, making it a genuine test of the mechanism, not just a vehicle for it.

---

## Architecture

```
producer.py                      consumer.py (N instances,
  |                                same group.id)
  | key = user_id                  |
  v                                | partition assignment handled
+--------------------------+       | automatically by the broker
|  Kafka topic:            |<------+
|  "clickstream"           |       |
|  4 partitions            |       v
+--------------------------+  +--------------+      +-------------------+
                               | active_      |<---->| session_closer    |
                               | sessions     |      | (periodic sweep,  |
                               | (Postgres)   |      |  no Kafka         |
                               +------+-------+      |  involvement)     |
                                      |               +--------+----------+
                                      v                        v
                               (row extended,           +--------------+
                                or new session           | closed_      |
                                started)                 | sessions     |
                                                          +--------------+
```

- **`producer.py`** — simulates 20 fake users. For each producer "burst," one user is picked and sent 3-8 click events in quick succession (real browsing sessions look like bursts, not uniform noise — this makes the pipeline's sessionization behavior visible when demoed, rather than a uniform random spray that's hard to eyeball).
- **`consumer.py`** — a single script; horizontal scaling is achieved by running multiple copies of it with the same `group.id`, not by writing any custom control-plane code (contrast with the Redis Streams project's `monitor.py`, which had to be built by hand).
- **`session_closer.py`** — a pure timer-driven sweep with zero Kafka involvement. Sessions close because time has passed with no new event, which is something only a clock-driven process can detect — a Kafka consumer can only react to messages that arrive, so closure logic structurally cannot live inside the consumer.

---

## Why Kafka instead of Redis Streams for this specific workload

Both are capable message brokers; the choice here was deliberate, not arbitrary. Redis Streams (used in the companion project) is a single ordered log per stream — there is no sub-division within it. A Kafka topic is split into **partitions**, each an independently ordered log. This has two consequences directly relevant to sessionization:

**Ordering only within a partition, not across the whole topic.** By keying every event on `user_id`, Kafka's default partitioner hashes the key and consistently routes all of one user's events to the same partition, guaranteeing they arrive at a consumer in the order they were sent. Events for different users may land in different partitions and have no ordering relationship to each other, which is fine and in fact desirable: sessionization only cares about order within a user, never across users.

**Consumer parallelism scales with partition count, and the broker manages it automatically.** A Redis Streams consumer group requires a hand-written control plane (see the companion project's `monitor.py`) to decide how many workers to run and to spawn/retire them via `multiprocessing`. A Kafka consumer group needs none of that: partition-to-consumer assignment is handled by the broker's group coordinator. Scaling this project's consumers is done by running more copies of the identical `consumer.py` script, with the same `group.id` — the broker automatically triggers a **rebalance**, redistributing the topic's partitions across however many consumer instances are currently alive. Running more consumer instances than there are partitions leaves the excess idle; this is a real, known constraint, not a bug (see Known Limitations).

---

## Design decisions and rejected alternatives

### Partition count: 4, chosen explicitly at topic-creation time (not left to auto-creation defaults)

```bash
kafka-topics --create --topic clickstream --bootstrap-server localhost:9092 \
  --partitions 4 --replication-factor 1
```

Partition count is fixed at topic-creation time and can only be increased later, never decreased — and increasing it after messages exist changes the key-to-partition hash mapping going forward, so it is not something to leave to chance or Kafka's auto-creation default (which defaults to a single partition, making the topic effectively unparallelizable). Four partitions gives headroom to demonstrate up to four concurrently useful consumer instances without being excessive for a local, single-broker demo.

### KRaft mode instead of Kafka + Zookeeper

Historically Kafka required a separate Zookeeper cluster for coordination metadata. Kafka 3.x+ supports KRaft mode, where the broker manages its own metadata with no external dependency. KRaft was used here both because it is simpler to run locally (one container instead of two) and because it is the direction Kafka itself is moving — Zookeeper mode is being phased out — so it is also the more current thing to know.

### Manual offset commits (`enable.auto.commit: False`), committed only after a successful Postgres write

Rejected alternative: `enable.auto.commit: True` (Kafka's default), which commits offsets on a fixed background schedule regardless of whether the consumer's own processing actually succeeded.

The failure mode this creates: if a Postgres write throws an exception after Kafka has already auto-committed that offset on its own schedule, the event is permanently gone from Kafka's perspective — it will never be redelivered — but Postgres never received the update. This is silent, undetectable data loss. With manual commit, the offset is only advanced immediately after `conn.commit()` succeeds; if the write fails, the commit call is simply never reached, and the exact same message is redelivered on the next poll (since Kafka still believes it was never processed). This is what makes the system's at-least-once guarantee actually true rather than aspirational.

### Timestamp-guarded conditional UPDATE, not a plain UPSERT

Rejected alternative: a single `INSERT ... ON CONFLICT (user_id) DO UPDATE SET last_event_at = NOW(), event_count = event_count + 1` — simpler, but incorrect for two reasons.

First, it always extends the existing session regardless of how much time has passed, which breaks the 30-minute inactivity rule entirely — a click arriving two hours after a user's last activity would silently extend the stale session rather than starting a new one. Second, it has no defense against out-of-order delivery during a partition rebalance: during the brief window where a partition transfers from one consumer to another, it's possible (though rare) for two consumers to both attempt to update the same user's row in close succession. The actual logic implemented is three distinct branches — no existing session (insert), existing but expired beyond 30 minutes (overwrite as a fresh session), existing and still active (extend) — with the extend branch guarded by:

```sql
UPDATE active_sessions
SET last_event_at = %s, event_count = event_count + 1
WHERE user_id = %s AND last_event_at < %s;
```

If a stale or reordered write arrives after a newer one has already landed, this WHERE clause matches zero rows and the update is a correct no-op — no explicit locking required; correctness falls entirely out of the WHERE condition. This is a standard last-write-wins-with-monotonicity-check pattern for exactly this class of problem.

### `session_closer.py` performs a full table scan per sweep, not a priority-queue-based expiry check

Rejected alternative: maintain an in-memory (or Redis-backed) min-heap of users ordered by expected expiry time, so the closer only ever looks at the single most-imminent expiry rather than scanning every active session. This is the theoretically more efficient approach, but it introduces real complexity: a heap ordered by last-activity-time does not naturally handle updates (a new click for a user already in the heap needs to reposition them, which a plain heap doesn't support in O(log n) without additional bookkeeping), and it introduces a second source of truth for session state that has to stay synchronized with Postgres.

The chosen approach — `DELETE FROM active_sessions WHERE last_event_at < NOW() - INTERVAL '30 minutes' RETURNING ...`, run every 60 seconds — is a full scan, but it's a correct, simple, single-source-of-truth implementation, and an index on `last_event_at` keeps the scan cheap even as the table grows, since Postgres can seek directly to the expired rows rather than scanning the whole table. At this project's scale (tens of thousands of rows, not billions), the simpler design was chosen deliberately over the more complex one — a stated tradeoff, not an oversight.

### One topic for clickstream events, not one shared topic across event domains

If this system were extended with, say, a payments service and an auth service, each would get its own topic (`payment-events`, `auth-events`), not share the `clickstream` topic. Different event domains generally need different partition keys (ordering-per-user makes sense for clicks; a payments topic might need ordering-per-transaction or ordering-per-account instead), different consumer-group scaling profiles, and different retention/compaction policies — none of which can be configured per-event-type within a single topic. The one legitimate exception to "separate topics per domain" is when two event types are so tightly coupled they must stay strictly ordered relative to each other (e.g., "order placed" and "order cancelled" for the same order) — in that case they'd share a topic and key, with an `event_type` field inside the payload used for consumer-side dispatch. This project has only one event type, so the question doesn't arise in the code, but the reasoning for how it would be handled is a deliberate design position, not an unconsidered gap.

---

## Bugs fixed during development

**All partitioning showing partition 0 despite different keys.** Root cause: the topic had been recreated with Kafka's auto-creation default of a single partition after a Docker volume wipe, silently overriding the explicit `--partitions 4` used the first time the topic was created. Fixed by explicitly deleting and recreating the topic with `--partitions 4` and confirming via `kafka-topics --describe` before re-running the producer.

**`enable.auto.commit` left at its default (`True`) despite the design requiring manual commits.** The consumer's manual-commit logic (`consumer.commit(message=msg, asynchronous=False)` after a successful DB write) was written correctly, but the config dict passed to `Consumer(...)` still had auto-commit enabled — meaning Kafka was periodically committing offsets on its own schedule in parallel with the manual commits, defeating the entire point of the design. Caught by explicitly re-checking the config dict against the intended design rather than assuming the manual commit call alone was sufficient.

**Timezone-naive vs. timezone-aware datetime comparison crash.** `active_sessions.last_event_at` is a TIMESTAMPTZ column, so psycopg2 returns it as a timezone-aware datetime. The producer originally emitted `datetime.now().isoformat()` with no timezone, producing a naive datetime on the consumer side after `datetime.fromisoformat()`. Comparing a naive and an aware datetime raises TypeError. Fixed by changing the producer to `datetime.now(timezone.utc).isoformat()`, making every timestamp in the system timezone-aware end to end.

**A bare `assert msg.value() is not None` would crash the entire consumer process on a single malformed message.** AssertionError is not caught by a surrounding `try/except Exception`, so one bad message would kill the whole long-running consumer rather than being skipped. Replaced with an explicit `if msg.value() is None: ... consumer.commit(...); continue`, so a malformed message is logged, acknowledged (to avoid an infinite redelivery loop on unparseable data), and skipped without taking down the process.

---

## Running locally

```bash
git clone https://github.com/SiddhantGupta3112/kafka-clickstream-pipeline.git
cd kafka-clickstream-pipeline
cp .env.example .env
docker compose up -d
```

Create the topic explicitly (do not rely on auto-creation):
```bash
docker compose exec kafka kafka-topics --create \
  --topic clickstream --bootstrap-server localhost:9092 \
  --partitions 4 --replication-factor 1
```

Run the pipeline (three separate terminals):
```bash
python producer.py
python consumer.py
python session_closer.py
```

**Demonstrate automatic rebalancing:** open a second terminal and run `python consumer.py` again — same `group.id`, a second live instance. Watch both terminals' `on_assign`/`on_revoke` callback output: the broker will trigger a rebalance and split the topic's 4 partitions across both consumers with no custom code involved.

Inspect session state directly:
```bash
docker compose exec postgres psql -U admin -d clickstream -c \
  "SELECT user_id, session_start, last_event_at, event_count FROM active_sessions ORDER BY last_event_at DESC;"
```

---

## Known limitations

- **No authentication on the Kafka broker (PLAINTEXT listener) or default Postgres in this local setup.** Any client that can reach the broker's port and knows the group name can join the consumer group — there is no SASL/mTLS authentication and no ACL-based authorization configured. In production this would require SASL/SCRAM or mTLS plus ACLs restricting which principals can join which groups or access which topics, typically layered on top of network-level isolation as the primary defense. Documented explicitly rather than left implicit.
- **Consumer group can have at most as many usefully active consumers as there are partitions (4).** A 5th or 6th consumer instance joining the same group sits idle, since partition assignment cannot subdivide a single partition across multiple consumers. This is an inherent property of Kafka's consumer-group model, not a limitation specific to this implementation.
- **`session_closer.py` uses a full table scan per sweep** rather than a priority-queue-based expiry mechanism — a deliberate simplicity tradeoff at this project's scale, detailed above.
- **Replication factor is 1** (single broker, appropriate for local development). A production deployment would use a multi-broker cluster with a replication factor of at least 3 for fault tolerance.