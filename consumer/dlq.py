import json
import logging

from confluent_kafka import Producer

from config import BOOTSTRAP_SERVERS, DEAD_LETTER_TOPIC

log = logging.getLogger("consumer")


def build_dlq_producer() -> Producer:
    return Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "acks": "1"})


def send_to_dlq(producer: Producer, topic: str, raw: bytes, reason: str) -> None:
    payload = json.dumps({
        "original_topic": topic,
        "reason": reason,
        "raw": raw.decode("utf-8", errors="replace"),
    }).encode()
    producer.produce(DEAD_LETTER_TOPIC, value=payload)
    producer.poll(0)
    log.warning("DLQ | topic=%s reason=%s", topic, reason)
