#!/bin/bash
# ──────────────────────────────────────────────────────────────
# init_kafka_topics.sh
# Creates all Kafka topics required by the pub/sub system.
# Runs once as a Docker one-shot container after Kafka is healthy.
# ──────────────────────────────────────────────────────────────

set -e

BOOTSTRAP="kafka-1:29092"
REPLICATION=3      # 3-broker KRaft cluster
RETENTION_MS=604800000  # 7 days

wait_for_kafka() {
  echo "[init] Waiting for Kafka broker at ${BOOTSTRAP} ..."
  until kafka-broker-api-versions --bootstrap-server "${BOOTSTRAP}" > /dev/null 2>&1; do
    sleep 2
  done
  echo "[init] Kafka is ready."
}

create_topic() {
  local name=$1
  local partitions=$2

  if kafka-topics --bootstrap-server "${BOOTSTRAP}" --list | grep -q "^${name}$"; then
    echo "[init] Topic '${name}' already exists — skipping."
  else
    kafka-topics \
      --bootstrap-server "${BOOTSTRAP}" \
      --create \
      --topic "${name}" \
      --partitions "${partitions}" \
      --replication-factor "${REPLICATION}" \
      --config retention.ms="${RETENTION_MS}"
    echo "[init] Created topic '${name}' (partitions=${partitions})"
  fi
}

wait_for_kafka

# ── Core business event topics ──────────────────────────────
# Partition count chosen based on expected event throughput per topic.
# In production, tune these based on consumer parallelism requirements.
create_topic "policy_events"   6  # highest volume: new policies, renewals
create_topic "claim_events"    4  # moderate volume: claims lifecycle
create_topic "premium_events"  4  # moderate volume: payment tracking
create_topic "dead_letter_events" 3  # invalid/failed messages from consumer

echo "[init] All Kafka topics created successfully."
