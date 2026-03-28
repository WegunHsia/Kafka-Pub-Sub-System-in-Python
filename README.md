# Kafka Pub/Sub System in Python

A containerized streaming pipeline simulating real-time insurance event processing. Events flow from a Python producer through a 3-broker Kafka cluster, get validated and enriched by a Python consumer, and land in PostgreSQL as analytics-ready views.

---

## Getting Started

```bash
cp .env.example .env
docker compose up -d
```

Everything is orchestrated via docker compose. `kafka-init` waits for all three brokers to be healthy before creating topics, and the consumer waits for both kafka-init and postgres to be ready — so startup order is handled automatically.

| Service | Port | Notes |
|---|---|---|
| kafka-1 | 9092 | Only broker exposed externally (kafka-2/3 are internal only) |
| postgres | 5432 | Analytics storage |
| kafka-ui | 8080 | Broker and topic monitoring |

---

## Architecture

```
Producer (Python)
    │  Generates 5 insurance events/sec across 3 topics
    ▼
Kafka Cluster (3-broker KRaft, no ZooKeeper)
    │
    ▼
Consumer (Python)
    │  Validate → Enrich → Batch write (flush every 50 rows)
    ▼
PostgreSQL
    ├─ raw event tables  (written by consumer)
    └─ analytical views  (pre-aggregated, plug into any BI tool)
```

---

## Why I Made These Choices

**KRaft instead of ZooKeeper**

ZooKeeper is a separate distributed system that needs its own maintenance and monitoring. KRaft moves the controller election logic inside Kafka itself — one less thing to run. Kafka 4.0 dropped ZooKeeper support entirely, so this is also just the direction things are heading.

**3 brokers**

Raft needs a majority vote to make decisions. With 3 brokers, you can lose 1 and still have quorum. 2 brokers is actually worse — if either one goes down you're stuck, and you get split-brain risk on top of that.

**At-least-once + ON CONFLICT DO NOTHING**

The consumer only commits offsets after a successful DB write. If it crashes mid-batch, it'll re-read and re-process on restart — no data loss. Duplicate writes are silently ignored by the `event_id` unique constraint. The end result is effectively exactly-once behavior without needing Kafka Transactions.

**Dead Letter Queue**

Bad messages can't be dropped silently and can't be allowed to block the pipeline. Anything that fails validation goes to `dead_letter_events` with the original payload and a reason, so it can be investigated and replayed later if needed.

---

## Data Model

The consumer writes directly to three raw tables:

- `raw_policy_events` — policy lifecycle (issued, renewed, lapsed, cancelled...)
- `raw_claim_events` — claims (submitted, reviewing, approved, rejected...)
- `raw_premium_events` — premium payments (due, paid, overdue, lapse warning...)

These are append-only. Nothing gets updated or deleted, which makes Kafka replays safe.

The analytics layer sits on top as views — no separate storage:

- `v_daily_new_policies` — daily new policy volume by product and channel
- `v_policy_status_snapshot` — current status distribution across all policies
- `v_monthly_claim_analysis` — approval rate, loss ratio, avg days to process
- `v_premium_collection_rate` — monthly collection rate and lapse risk count
- `v_customer_360` — per-customer coverage, claims, tier (PLATINUM / GOLD / SILVER / STANDARD)

---

## If This Were Production

The core issue is OLTP and OLAP sharing the same PostgreSQL — analytical queries compete directly with incoming writes. At scale, the storage layer would move to BigQuery or an open-source Lakehouse (Iceberg + Trino), the Python consumer would be replaced by a managed stream processor (Dataflow or Flink), and the SQL views would become dbt models. Kafka stays as the central event bus throughout — multiple consumer groups can read the same topics independently without duplicating data.
