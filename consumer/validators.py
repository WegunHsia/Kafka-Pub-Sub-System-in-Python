from models import ClaimEvent, PolicyEvent, PremiumEvent

VALID_CHANNELS = {"AGENT", "BANCASSURANCE", "DIRECT", "DIGITAL"}


def validate_policy(e: PolicyEvent) -> list[str]:
    errors = []
    if e.channel not in VALID_CHANNELS:
        errors.append(f"unknown channel: {e.channel}")
    if e.coverage_amount <= 0:
        errors.append("coverage_amount must be positive")
    if e.annual_premium <= 0:
        errors.append("annual_premium must be positive")
    return errors


def validate_claim(e: ClaimEvent) -> list[str]:
    errors = []
    if e.claim_amount <= 0:
        errors.append("claim_amount must be positive")
    if e.event_type == "CLAIM_APPROVED" and e.approved_amount <= 0:
        errors.append("approved_amount must be positive for CLAIM_APPROVED")
    if e.event_type == "CLAIM_REJECTED" and not e.rejection_reason:
        errors.append("rejection_reason required for CLAIM_REJECTED")
    return errors


def validate_premium(e: PremiumEvent) -> list[str]:
    errors = []
    if e.amount <= 0:
        errors.append("amount must be positive")
    if e.event_type == "PREMIUM_PAID" and not e.payment_date:
        errors.append("payment_date required for PREMIUM_PAID")
    if e.days_overdue < 0:
        errors.append("days_overdue cannot be negative")
    return errors


VALIDATORS = {
    "policy_events":  validate_policy,
    "claim_events":   validate_claim,
    "premium_events": validate_premium,
}
