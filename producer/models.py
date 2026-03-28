from enum import Enum
from pydantic import BaseModel


class PolicyEventType(str, Enum):
    ISSUED      = "POLICY_ISSUED"
    RENEWED     = "POLICY_RENEWED"
    LAPSED      = "POLICY_LAPSED"
    SURRENDERED = "POLICY_SURRENDERED"
    CANCELLED   = "POLICY_CANCELLED"


class ClaimEventType(str, Enum):
    SUBMITTED = "CLAIM_SUBMITTED"
    REVIEWING = "CLAIM_REVIEWING"
    APPROVED  = "CLAIM_APPROVED"
    REJECTED  = "CLAIM_REJECTED"
    PAID      = "CLAIM_PAID"


class PremiumEventType(str, Enum):
    DUE          = "PREMIUM_DUE"
    PAID         = "PREMIUM_PAID"
    OVERDUE      = "PREMIUM_OVERDUE"
    GRACE_PERIOD = "PREMIUM_GRACE_PERIOD"
    LAPSE_WARN   = "LAPSE_WARNING"


class PolicyEvent(BaseModel):
    event_id:          str
    event_type:        PolicyEventType
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
    event_type:       ClaimEventType
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
    event_type:       PremiumEventType
    event_timestamp:  int
    policy_number:    str
    customer_id:      str
    due_date:         str
    payment_date:     str | None
    amount:           int
    payment_method:   str
    days_overdue:     int
