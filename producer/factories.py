import random
import uuid
from datetime import date, datetime, timedelta

from config import CHANNELS, CLAIM_TYPES, PAYMENT_METHODS, PRODUCTS
from models import (
    ClaimEvent, ClaimEventType,
    PolicyEvent, PolicyEventType,
    PremiumEvent, PremiumEventType,
)


def _now_ms() -> int:
    return int(datetime.utcnow().timestamp() * 1000)

def _policy_number() -> str:
    return f"PRU-{date.today().year}-{random.randint(100000, 999999)}"

def _customer_id() -> str:
    return f"CUST-{random.randint(1000, 9999)}"

def _agent_id() -> str:
    return f"AGT-{random.randint(100, 999)}"

def _claim_id() -> str:
    return f"CLM-{date.today().year}-{random.randint(10000, 99999)}"


def make_policy_event() -> tuple[str, PolicyEvent]:
    product_code, product_name = random.choice(PRODUCTS)

    if "TERM" in product_code or "WHOLE" in product_code:
        coverage = random.randint(1_000_000, 10_000_000)
        premium  = int(coverage * random.uniform(0.003, 0.008))
    else:
        coverage = random.randint(200_000, 2_000_000)
        premium  = random.randint(5_000, 50_000)

    start = date.today() - timedelta(days=random.randint(0, 730))
    years = int(product_code.split("-")[-1]) if product_code[-2:].isdigit() else 20
    end   = start + timedelta(days=years * 365)

    event = PolicyEvent(
        event_id=str(uuid.uuid4()),
        event_type=random.choices(
            list(PolicyEventType),
            weights=[50, 20, 10, 10, 10],
        )[0],
        event_timestamp=_now_ms(),
        policy_number=_policy_number(),
        customer_id=_customer_id(),
        product_code=product_code,
        product_name=product_name,
        coverage_amount=coverage,
        annual_premium=premium,
        policy_start_date=start.isoformat(),
        policy_end_date=end.isoformat(),
        agent_id=_agent_id(),
        channel=random.choice(CHANNELS),
    )
    return event.customer_id, event


def make_claim_event() -> tuple[str, ClaimEvent]:
    event_type = random.choices(
        list(ClaimEventType),
        weights=[30, 25, 20, 10, 15],
    )[0]

    claim_amount    = random.randint(50_000, 5_000_000)
    approved_amount = (
        int(claim_amount * random.uniform(0.8, 1.0))
        if event_type in (ClaimEventType.APPROVED, ClaimEventType.PAID)
        else 0
    )
    rejection_reason = (
        random.choice(["PRE_EXISTING_CONDITION", "POLICY_LAPSED", "FRAUD_SUSPECTED", "INCOMPLETE_DOCS"])
        if event_type == ClaimEventType.REJECTED
        else None
    )

    customer_id = _customer_id()
    event = ClaimEvent(
        event_id=str(uuid.uuid4()),
        event_type=event_type,
        event_timestamp=_now_ms(),
        claim_id=_claim_id(),
        policy_number=_policy_number(),
        customer_id=customer_id,
        claim_type=random.choice(CLAIM_TYPES),
        claim_amount=claim_amount,
        approved_amount=approved_amount,
        rejection_reason=rejection_reason,
        days_to_process=random.randint(1, 30),
    )
    return customer_id, event


def make_premium_event() -> tuple[str, PremiumEvent]:
    event_type = random.choices(
        list(PremiumEventType),
        weights=[25, 45, 15, 10, 5],
    )[0]

    due_date     = date.today() - timedelta(days=random.randint(0, 30))
    days_overdue = (
        random.randint(1, 60)
        if event_type in (PremiumEventType.OVERDUE, PremiumEventType.GRACE_PERIOD, PremiumEventType.LAPSE_WARN)
        else 0
    )
    payment_date = (
        (due_date + timedelta(days=random.randint(0, 5))).isoformat()
        if event_type == PremiumEventType.PAID
        else None
    )

    customer_id = _customer_id()
    event = PremiumEvent(
        event_id=str(uuid.uuid4()),
        event_type=event_type,
        event_timestamp=_now_ms(),
        policy_number=_policy_number(),
        customer_id=customer_id,
        due_date=due_date.isoformat(),
        payment_date=payment_date,
        amount=random.randint(5_000, 100_000),
        payment_method=random.choice(PAYMENT_METHODS),
        days_overdue=days_overdue,
    )
    return customer_id, event
