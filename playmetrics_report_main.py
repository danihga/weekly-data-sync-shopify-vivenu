import os
from io import StringIO
from dotenv import load_dotenv
import pandas as pd
from google.cloud import storage

load_dotenv()

TIFC_INSTANCES = {
    "TIFC East": os.getenv("PM_BUCKET_TIFC_EAST", "club1811-reports-playmetrics-prod"),
    "TIFC West": os.getenv("PM_BUCKET_TIFC_WEST"),  # None until PlayMetrics provides bucket
}


def _get_client() -> storage.Client:
    return storage.Client()


def _fetch_emails_from_bucket(bucket_name: str) -> list[str]:
    client = _get_client()
    blob = client.bucket(bucket_name).blob("all_player_contacts.csv")
    df = pd.read_csv(StringIO(blob.download_as_text()), dtype=str)
    return (
        df["contact_email"]
        .dropna()
        .str.lower()
        .str.strip()
        .loc[lambda s: s != ""]
        .unique()
        .tolist()
    )


def fetch_playmetrics_contacts() -> dict:
    """
    Fetches contact emails from all configured TIFC PlayMetrics instances.
    Instances without a bucket (WIP) are included in the summary but skipped for data.
    Returns per-instance counts and a combined email list for deduplication.
    """
    all_emails = []
    instances = []

    for name, bucket in TIFC_INSTANCES.items():
        if not bucket:
            instances.append({"name": name, "count": None, "wip": True})
            continue

        try:
            emails = _fetch_emails_from_bucket(bucket)
            all_emails.extend(emails)
            instances.append({"name": name, "count": len(emails), "wip": False})
            print(f"  PlayMetrics {name}: {len(emails)} unique contacts")
        except Exception as e:
            print(f"  ⚠️  PlayMetrics {name}: could not fetch — {e}")
            instances.append({"name": name, "count": None, "wip": True})

    return {
        "instances": instances,
        "emails": list(set(all_emails)),
        "count": len(set(all_emails)),
    }
