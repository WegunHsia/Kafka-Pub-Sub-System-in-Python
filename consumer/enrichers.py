def enrich_claim(data: dict) -> dict:
    ca = data.get("claim_amount", 0)
    aa = data.get("approved_amount", 0)
    data["loss_ratio"] = round(aa / ca, 4) if ca > 0 else 0.0
    return data


def enrich_premium(data: dict) -> dict:
    data["is_lapse_risk"] = data.get("days_overdue", 0) >= 30
    return data


ENRICHERS = {
    "claim_events":   enrich_claim,
    "premium_events": enrich_premium,
}
