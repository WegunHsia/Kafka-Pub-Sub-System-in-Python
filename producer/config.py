import os

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
EVENTS_PER_SECOND = float(os.environ.get("EVENTS_PER_SECOND", "5"))
LOG_LEVEL         = os.environ.get("LOG_LEVEL", "INFO").upper()

TOPIC_POLICY  = "policy_events"
TOPIC_CLAIM   = "claim_events"
TOPIC_PREMIUM = "premium_events"

PRODUCTS = [
    ("LIFE-TERM-20", "定期壽險20年"),
    ("LIFE-WHOLE",   "終身壽險"),
    ("CI-MAJOR",     "重大疾病險"),
    ("HOSP-DAILY",   "住院日額險"),
    ("ANNUITY-DFR",  "利率變動型年金"),
]

CHANNELS        = ["AGENT", "BANCASSURANCE", "DIRECT", "DIGITAL"]
CLAIM_TYPES     = ["DEATH", "DISABILITY", "CRITICAL_ILLNESS", "HOSPITALIZATION", "MATURITY"]
PAYMENT_METHODS = ["CREDIT_CARD", "BANK_TRANSFER", "AUTO_DEBIT"]
