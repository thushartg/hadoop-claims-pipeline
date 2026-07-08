"""
generate_data.py

Generates a SYNTHETIC health insurance claims dataset. All members,
providers, diagnoses, and claims below are randomly generated -- this is
not real patient data, not derived from any real PHI/PII source, and
contains no real member or provider identities. It stands in for the raw
claims feed that would normally land in HDFS from a payer's claims
adjudication system (e.g. via a nightly extract job or Sqoop/Kafka Connect
ingestion from a legacy claims processing system).

Usage:
    python3 generate_data.py --num-members 8000 --num-providers 400 \
        --num-claims 150000 --out ../raw/claims.csv

Output columns:
    claim_id, member_id, provider_id, provider_type, plan_type,
    member_age_band, member_state, claim_type, diagnosis_category,
    procedure_category, service_date, submission_date, billed_amount,
    allowed_amount, paid_amount, claim_status, denial_reason,
    is_readmission_within_30d
"""
import argparse
import csv
import random
import uuid
from datetime import datetime, timedelta

PROVIDER_TYPES = ["hospital", "physician_office", "pharmacy", "lab", "urgent_care", "specialist"]
PLAN_TYPES = ["HMO", "PPO", "EPO", "Medicare Advantage", "Medicaid MCO"]
AGE_BANDS = ["0-17", "18-34", "35-49", "50-64", "65-79", "80+"]
STATES = ["CA", "TX", "NY", "FL", "PA", "OH", "GA", "NC", "MI", "AZ"]
CLAIM_TYPES = ["inpatient", "outpatient", "pharmacy", "professional", "emergency"]

DIAGNOSIS_CATEGORIES = [
    "diabetes", "hypertension", "cardiovascular_disease", "respiratory_illness",
    "musculoskeletal", "mental_health", "oncology", "maternity", "injury_trauma",
    "preventive_care", "infectious_disease", "chronic_kidney_disease",
]

PROCEDURE_CATEGORIES = [
    "office_visit", "diagnostic_imaging", "lab_panel", "surgical_procedure",
    "physical_therapy", "emergency_treatment", "inpatient_stay", "prescription_fill",
    "specialist_consult", "preventive_screening",
]

DENIAL_REASONS = [
    "prior_authorization_missing", "not_medically_necessary", "out_of_network",
    "duplicate_claim", "coverage_terminated", "incomplete_documentation",
    "non_covered_service",
]

# Rough relative average billed cost by claim_type, used to make amounts
# plausible (inpatient >> professional, etc.)
CLAIM_TYPE_COST_RANGE = {
    "inpatient": (8000, 65000),
    "emergency": (1500, 18000),
    "outpatient": (400, 6000),
    "professional": (80, 900),
    "pharmacy": (15, 1200),
}


def build_providers(num_providers):
    providers = []
    for i in range(num_providers):
        provider_id = f"PRV-{i:05d}"
        provider_type = random.choice(PROVIDER_TYPES)
        # A handful of providers are deliberately made cost outliers to
        # give the "provider anomaly" analysis something real to find.
        is_outlier = random.random() < 0.03
        providers.append((provider_id, provider_type, is_outlier))
    return providers


def build_members(num_members):
    members = []
    for i in range(num_members):
        member_id = f"MBR-{i:06d}"
        members.append({
            "member_id": member_id,
            "plan_type": random.choice(PLAN_TYPES),
            "age_band": random.choices(AGE_BANDS, weights=[10, 20, 22, 24, 18, 6])[0],
            "state": random.choice(STATES),
        })
    return members


def generate_claim(member, providers, claim_date, prior_inpatient_claims):
    provider_id, provider_type, is_outlier_provider = random.choice(providers)
    claim_type = random.choices(
        CLAIM_TYPES, weights=[8, 22, 25, 35, 10]
    )[0]
    diagnosis_category = random.choice(DIAGNOSIS_CATEGORIES)
    procedure_category = random.choice(PROCEDURE_CATEGORIES)

    low, high = CLAIM_TYPE_COST_RANGE[claim_type]
    billed = round(random.uniform(low, high), 2)
    if is_outlier_provider:
        billed = round(billed * random.uniform(2.0, 4.5), 2)

    # Adjudication: most claims get paid at some allowed-amount discount,
    # some are denied outright.
    denial_roll = random.random()
    if denial_roll < 0.08:
        status = "denied"
        allowed = 0.0
        paid = 0.0
        denial_reason = random.choice(DENIAL_REASONS)
    else:
        allowed_pct = random.uniform(0.55, 0.9)
        allowed = round(billed * allowed_pct, 2)
        if denial_roll < 0.12:
            status = "partially_paid"
            paid = round(allowed * random.uniform(0.4, 0.85), 2)
        else:
            status = "paid"
            paid = allowed
        denial_reason = ""

    submission_date = claim_date + timedelta(days=random.randint(0, 14))

    is_readmission = False
    if claim_type == "inpatient":
        for prev_date in prior_inpatient_claims.get(member["member_id"], []):
            if 0 < (claim_date - prev_date).days <= 30:
                is_readmission = True
                break
        prior_inpatient_claims.setdefault(member["member_id"], []).append(claim_date)

    return {
        "claim_id": str(uuid.uuid4()),
        "member_id": member["member_id"],
        "provider_id": provider_id,
        "provider_type": provider_type,
        "plan_type": member["plan_type"],
        "member_age_band": member["age_band"],
        "member_state": member["state"],
        "claim_type": claim_type,
        "diagnosis_category": diagnosis_category,
        "procedure_category": procedure_category,
        "service_date": claim_date.date().isoformat(),
        "submission_date": submission_date.date().isoformat(),
        "billed_amount": billed,
        "allowed_amount": allowed,
        "paid_amount": paid,
        "claim_status": status,
        "denial_reason": denial_reason,
        "is_readmission_within_30d": is_readmission,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-members", type=int, default=8000)
    parser.add_argument("--num-providers", type=int, default=400)
    parser.add_argument("--num-claims", type=int, default=150000)
    parser.add_argument("--days", type=int, default=180)
    parser.add_argument("--out", type=str, default="../raw/claims.csv")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    random.seed(args.seed)
    providers = build_providers(args.num_providers)
    members = build_members(args.num_members)

    end_date = datetime(2026, 7, 6)
    start_date = end_date - timedelta(days=args.days)

    fieldnames = [
        "claim_id", "member_id", "provider_id", "provider_type", "plan_type",
        "member_age_band", "member_state", "claim_type", "diagnosis_category",
        "procedure_category", "service_date", "submission_date", "billed_amount",
        "allowed_amount", "paid_amount", "claim_status", "denial_reason",
        "is_readmission_within_30d",
    ]

    prior_inpatient_claims = {}

    with open(args.out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for _ in range(args.num_claims):
            member = random.choice(members)
            offset_days = random.randint(0, args.days)
            claim_date = start_date + timedelta(days=offset_days, hours=random.randint(0, 23))
            row = generate_claim(member, providers, claim_date, prior_inpatient_claims)
            writer.writerow(row)

    print(f"Wrote {args.num_claims} claims to {args.out}")


if __name__ == "__main__":
    main()
