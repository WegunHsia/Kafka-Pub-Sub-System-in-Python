import os

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
GROUP_ID          = os.environ.get("KAFKA_GROUP_ID", "prudential-consumer-group")
DATABASE_URL      = os.environ.get("DATABASE_URL", "postgresql://pru_user:pru_pass@localhost:5432/prudential")
LOG_LEVEL         = os.environ.get("LOG_LEVEL", "INFO").upper()

TOPICS            = ["policy_events", "claim_events", "premium_events"]
DEAD_LETTER_TOPIC = "dead_letter_events"
BATCH_SIZE        = 50
POLL_TIMEOUT      = 1.0
