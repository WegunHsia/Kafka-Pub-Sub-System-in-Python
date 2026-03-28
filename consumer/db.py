import logging

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

log = logging.getLogger("consumer")

INSERT_POLICY = text("""
    INSERT INTO raw_policy_events
        (event_id, event_type, event_timestamp, policy_number, customer_id,
         product_code, product_name, coverage_amount, annual_premium,
         policy_start_date, policy_end_date, agent_id, channel)
    VALUES
        (:event_id, :event_type,
         to_timestamp(:event_timestamp / 1000.0),
         :policy_number, :customer_id,
         :product_code, :product_name,
         :coverage_amount, :annual_premium,
         :policy_start_date, :policy_end_date,
         :agent_id, :channel)
    ON CONFLICT (event_id) DO NOTHING
""")

INSERT_CLAIM = text("""
    INSERT INTO raw_claim_events
        (event_id, event_type, event_timestamp, claim_id, policy_number,
         customer_id, claim_type, claim_amount, approved_amount,
         rejection_reason, days_to_process, loss_ratio)
    VALUES
        (:event_id, :event_type,
         to_timestamp(:event_timestamp / 1000.0),
         :claim_id, :policy_number, :customer_id,
         :claim_type, :claim_amount, :approved_amount,
         :rejection_reason, :days_to_process, :loss_ratio)
    ON CONFLICT (event_id) DO NOTHING
""")

INSERT_PREMIUM = text("""
    INSERT INTO raw_premium_events
        (event_id, event_type, event_timestamp, policy_number, customer_id,
         due_date, payment_date, amount, payment_method, days_overdue, is_lapse_risk)
    VALUES
        (:event_id, :event_type,
         to_timestamp(:event_timestamp / 1000.0),
         :policy_number, :customer_id,
         :due_date, :payment_date,
         :amount, :payment_method,
         :days_overdue, :is_lapse_risk)
    ON CONFLICT (event_id) DO NOTHING
""")

TOPIC_INSERT = {
    "policy_events":  INSERT_POLICY,
    "claim_events":   INSERT_CLAIM,
    "premium_events": INSERT_PREMIUM,
}


class PostgresBatchWriter:
    def __init__(self, dsn: str) -> None:
        self._engine  = create_engine(dsn, pool_pre_ping=True)
        self._buffers: dict[str, list[dict]] = {}
        # track highest offset per (topic, partition) in buffer
        # only cleared after successful DB flush
        self._offsets: dict[tuple[str, int], int] = {}
        log.info("PostgreSQL connected.")

    def add(self, topic: str, record: dict, partition: int, offset: int) -> None:
        self._buffers.setdefault(topic, []).append(record)
        self._track_offset(topic, partition, offset)

    def mark_processed(self, topic: str, partition: int, offset: int) -> None:
        """Only update offset tracking, without writing to DB.
        Used for DLQ-routed or filtered messages so their offsets
        are committed together with the next successful DB flush.
        """
        self._track_offset(topic, partition, offset)

    def _track_offset(self, topic: str, partition: int, offset: int) -> None:
        key = (topic, partition)
        if offset > self._offsets.get(key, -1):
            self._offsets[key] = offset

    def size(self, topic: str) -> int:
        return len(self._buffers.get(topic, []))

    def flush(self, topic: str) -> list[tuple[str, int, int]] | None:
        """Flush topic buffer to DB.
        Returns list of (topic, partition, offset) to commit, or None on failure.
        """
        records = self._buffers.pop(topic, [])
        if not records:
            return []

        sql = TOPIC_INSERT[topic]
        try:
            with Session(self._engine) as session:
                session.execute(sql, records)
                session.commit()
            log.info("DB flush | topic=%-20s rows=%d", topic, len(records))

            # collect offsets for this topic and clear them
            to_commit = [
                (t, p, o) for (t, p), o in self._offsets.items() if t == topic
            ]
            for key in [(t, p) for t, p, _ in to_commit]:
                del self._offsets[key]
            return to_commit

        except Exception as exc:
            log.error("DB flush failed | topic=%s err=%s", topic, exc)
            self._buffers.setdefault(topic, []).extend(records)
            return None

    def flush_all(self) -> dict[tuple[str, int], int]:
        """Flush all topics. Returns all offsets ready to commit."""
        ready = {}
        for topic in list(self._buffers.keys()):
            result = self.flush(topic)
            if result:
                for t, p, o in result:
                    ready[(t, p)] = o
        return ready

    def close(self) -> None:
        self.flush_all()
        self._engine.dispose()