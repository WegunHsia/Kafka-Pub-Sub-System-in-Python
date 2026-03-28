import logging
import os
import random
import signal
import time
from typing import Any

from confluent_kafka import KafkaException, Producer

from config import (
    BOOTSTRAP_SERVERS, EVENTS_PER_SECOND, LOG_LEVEL,
    TOPIC_CLAIM, TOPIC_POLICY, TOPIC_PREMIUM,
)
from factories import make_claim_event, make_policy_event, make_premium_event

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("producer")


def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers":  BOOTSTRAP_SERVERS,
        "acks":               "all",
        "retries":            5,
        "retry.backoff.ms":   300,
        "compression.type":   "zstd",  # snappy, lz4...
        "linger.ms":          20,
        "batch.size":         65536, # 64kb
        # enable.idempotence requires a stable coordinator.
        # Set to True in production; disable locally if broker just started.
        "enable.idempotence": os.environ.get("KAFKA_IDEMPOTENCE", "true").lower() == "true",
    })


def delivery_report(err: Any, msg: Any) -> None:
    if err:
        log.error("Delivery failed | topic=%s partition=%s err=%s",
                  msg.topic(), msg.partition(), err)
    else:
        log.debug("Delivered | topic=%s partition=%s offset=%s",
                  msg.topic(), msg.partition(), msg.offset())


EVENT_FACTORIES = [
    (TOPIC_POLICY,  make_policy_event,  50),
    (TOPIC_PREMIUM, make_premium_event, 35),
    (TOPIC_CLAIM,   make_claim_event,   15),
]


def run() -> None:
    log.info("Starting producer | bootstrap=%s rate=%.1f eps",
             BOOTSTRAP_SERVERS, EVENTS_PER_SECOND)

    producer       = build_producer()
    shutdown       = False
    published      = 0
    sleep_interval = 1.0 / EVENTS_PER_SECOND
    topics, factories, weights = zip(*EVENT_FACTORIES)

    def _handler(sig, frame):
        nonlocal shutdown
        log.info("Shutdown signal received, flushing ...")
        shutdown = True

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)

    while not shutdown:
        try:
            factory = random.choices(factories, weights=weights)[0]
            topic   = topics[factories.index(factory)]
            key, event = factory()

            producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=event.model_dump_json().encode("utf-8"),
                on_delivery=delivery_report,
            )

            published += 1
            if published % 100 == 0:
                log.info("Published %d events", published)
                producer.poll(0)

            time.sleep(sleep_interval)

        except KafkaException as exc:
            log.error("Kafka error: %s", exc)
            time.sleep(2)
        except Exception as exc:
            log.exception("Unexpected error: %s", exc)
            time.sleep(2)

    producer.flush(timeout=10)
    log.info("Producer stopped. Total published: %d", published)


if __name__ == "__main__":
    run()
