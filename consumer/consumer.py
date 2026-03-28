"""
Kafka Event Consumer
=================================================
Consumes insurance events from Kafka topics, applies validation
and enrichment, then writes to PostgreSQL (append-only raw tables).

Flow:
  Kafka → validate (Pydantic) → business rules → enrich → PostgreSQL

Design decisions:
  - Manual offset commit (enable.auto.commit=false): we only commit
    after a successful DB write — at-least-once semantics.
  - UPSERT on event_id (ON CONFLICT DO NOTHING): idempotent writes,
    safe to replay Kafka messages without duplicating rows.
  - Batch insert (executemany): reduces DB round-trips.
  - Dead-letter topic: invalid events are routed to dead_letter_events
    instead of being silently dropped.
  - Connection pool (sqlalchemy): reuses DB connections across batches.
"""

from __future__ import annotations

import json
import logging
import signal
import time

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from pydantic import ValidationError

from config import (
    BOOTSTRAP_SERVERS, DATABASE_URL, GROUP_ID,
    LOG_LEVEL, POLL_TIMEOUT, BATCH_SIZE, TOPICS,
)
from db import PostgresBatchWriter
from dlq import build_dlq_producer, send_to_dlq
from enrichers import ENRICHERS
from models import TOPIC_MODELS
from validators import VALIDATORS

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("consumer")


def build_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers":     BOOTSTRAP_SERVERS,
        "group.id":              GROUP_ID,
        "auto.offset.reset":     "earliest",
        "enable.auto.commit":    False,
        "max.poll.interval.ms":  300_000,
        "session.timeout.ms":    30_000,
        "heartbeat.interval.ms": 10_000,
    })


def run() -> None:
    log.info("Starting consumer | bootstrap=%s group=%s topics=%s",
             BOOTSTRAP_SERVERS, GROUP_ID, TOPICS)

    consumer  = build_consumer()
    dlq       = build_dlq_producer()
    db        = PostgresBatchWriter(DATABASE_URL)
    shutdown  = False
    processed = 0
    errors    = 0

    def _handler(sig, frame):
        nonlocal shutdown
        log.info("Shutdown signal received ...")
        shutdown = True

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)

    consumer.subscribe(TOPICS)

    try:
        while not shutdown:
            msg = consumer.poll(POLL_TIMEOUT)

            if msg is None:
                ready = db.flush_all()
                for (t, partition), offset in ready.items():
                    consumer.commit(offsets=[TopicPartition(t, partition, offset + 1)])
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    log.debug("EOF | topic=%s partition=%s", msg.topic(), msg.partition())
                else:
                    log.error("Consumer error: %s", msg.error())
                continue

            topic = msg.topic()
            raw   = msg.value()

            # ── 1. Parse JSON ────────────────────────────────
            try:
                data = json.loads(raw)
            except json.JSONDecodeError as exc:
                send_to_dlq(dlq, topic, raw, f"json_parse_error: {exc}")
                db.mark_processed(topic, msg.partition(), msg.offset())
                errors += 1
                continue

            log.info("[STEP 1] raw bytes → dict | topic=%s keys=%s", topic, list(data.keys()))

            # ── 2. Schema validation (Pydantic) ─────────────
            model_cls = TOPIC_MODELS.get(topic)
            if not model_cls:
                send_to_dlq(dlq, topic, raw, f"unknown_topic: {topic}")
                db.mark_processed(topic, msg.partition(), msg.offset())
                errors += 1
                continue

            try:
                event = model_cls(**data)
            except ValidationError as exc:
                send_to_dlq(dlq, topic, raw,
                            f"schema_validation: {exc.error_count()} error(s)")
                db.mark_processed(topic, msg.partition(), msg.offset())
                errors += 1
                continue

            log.info("[STEP 2] Pydantic OK | type=%s event_id=%s event_type=%s",
                     type(event).__name__, event.event_id, event.event_type)

            # ── 3. Business rule validation ──────────────────
            validator  = VALIDATORS.get(topic)
            biz_errors = validator(event) if validator else []
            if biz_errors:
                send_to_dlq(dlq, topic, raw,
                            f"business_validation: {'; '.join(biz_errors)}")
                db.mark_processed(topic, msg.partition(), msg.offset())
                errors += 1
                continue

            log.info("[STEP 3] Business rules OK | no errors")

            # ── 4. Enrichment ────────────────────────────────
            record   = event.model_dump()
            enricher = ENRICHERS.get(topic)
            if enricher:
                record = enricher(record)
                log.info("[STEP 4] Enriched | added keys=%s",
                         [k for k in record if k not in event.model_fields])

            # ── 5. Buffer (offset tracked, not yet committed) ─
            db.add(topic, record, msg.partition(), msg.offset())
            processed += 1

            log.info("[STEP 5] Buffered | topic=%s buffer_size=%d/%d",
                     topic, db.size(topic), BATCH_SIZE)

            # ── 6. Flush + commit only after DB success ───────
            if db.size(topic) >= BATCH_SIZE:
                committed = db.flush(topic)
                if committed is None:
                    log.warning("DB unavailable, retrying next poll ...")
                    time.sleep(2)
                    continue
                for t, partition, offset in committed:
                    consumer.commit(offsets=[
                        TopicPartition(t, partition, offset + 1)
                    ])
                log.info("[STEP 6] Flushed & committed | topic=%s", topic)

            if processed % 500 == 0:
                log.info("Stats | processed=%d errors=%d", processed, errors)

    except KafkaException as exc:
        log.exception("Fatal Kafka error: %s", exc)
    finally:
        log.info("Performing final flush before shutdown...")
        ready = db.flush_all()
        if ready:
            try:
                commits = [TopicPartition(t, p, o + 1) for (t, p), o in ready.items()]
                consumer.commit(offsets=commits, asynchronous=False)
            except KafkaException as exc:
                log.warning("Final commit failed: %s", exc)
        db._engine.dispose()
        dlq.flush(timeout=5)
        consumer.close()
        log.info("Consumer stopped | processed=%d errors=%d", processed, errors)


if __name__ == "__main__":
    run()
