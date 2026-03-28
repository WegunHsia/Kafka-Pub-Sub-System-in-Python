from pydantic import BaseModel


class PolicyEvent(BaseModel):
    event_id:          str
    event_type:        str
    event_timestamp:   int
    policy_number:     str
    customer_id:       str
    product_code:      str
    product_name:      str
    coverage_amount:   int
    annual_premium:    int
    policy_start_date: str
    policy_end_date:   str
    agent_id:          str
    channel:           str


class ClaimEvent(BaseModel):
    event_id:         str
    event_type:       str
    event_timestamp:  int
    claim_id:         str
    policy_number:    str
    customer_id:      str
    claim_type:       str
    claim_amount:     int
    approved_amount:  int
    rejection_reason: str | None
    days_to_process:  int


class PremiumEvent(BaseModel):
    event_id:         str
    event_type:       str
    event_timestamp:  int
    policy_number:    str
    customer_id:      str
    due_date:         str
    payment_date:     str | None
    amount:           int
    payment_method:   str
    days_overdue:     int


TOPIC_MODELS: dict[str, type[BaseModel]] = {
    "policy_events":  PolicyEvent,
    "claim_events":   ClaimEvent,
    "premium_events": PremiumEvent,
}
