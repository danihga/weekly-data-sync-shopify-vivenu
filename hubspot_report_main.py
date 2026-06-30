import requests
import os
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

HUBSPOT_API_KEY = os.getenv("HUBSPOT_API_KEY")

TIFC_FORMS = {
    "Newsletter": "aa1c3ab2-cbb1-490e-b7a8-ef759c3b6373",
    "General Inquiries": "3c37d569-2a4b-4265-a61b-29551de791cb",
    "All Inquiries": "f428ff00-eb9c-4c5e-8ba0-bd76ff1f411c",
    "Supporters": "f8633ee3-cce1-4e90-917c-1adfde9b3dbb",
    "Club Affiliates": "88fa23ba-aab6-4e1c-8a54-6b63cc5196be",
    "Partnership": "dd37d8ad-098f-463d-a96b-29313706a5b2",
    "Women's Team Partnership": "70c3783c-ef34-438d-9843-d5d02221f027",
    "Career Expression of Interest": "a0c857d0-10b0-409d-90e0-6e3f39b24552",
}


def _fetch_all_form_submissions(form_id: str) -> list[dict]:
    headers = {"Authorization": f"Bearer {HUBSPOT_API_KEY}"}
    url = f"https://api.hubapi.com/form-integrations/v1/submissions/forms/{form_id}"
    all_submissions = []
    after = None

    while True:
        params = {"limit": 50}
        if after:
            params["after"] = after

        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()

        all_submissions.extend(data.get("results", []))

        next_cursor = data.get("paging", {}).get("next", {}).get("after")
        if not next_cursor:
            break
        after = next_cursor

    return all_submissions


def _extract_email(submission: dict) -> str | None:
    for field in submission.get("values", []):
        if field.get("name") == "email":
            val = field.get("value", "").lower().strip()
            return val if val else None
    return None


def fetch_hubspot_database(days_for_new: int = 7) -> dict:
    """
    Fetch all TIFC HubSpot form submissions.
    Returns per-form unique email counts and a combined email->first_seen_ms mapping
    for use in cross-source deduplication.
    """
    cutoff_ms = (datetime.now(timezone.utc) - timedelta(days=days_for_new)).timestamp() * 1000

    form_results = []
    # email -> earliest submittedAt ms across all TIFC forms
    email_first_seen: dict[str, float] = {}

    for form_name, form_id in TIFC_FORMS.items():
        submissions = _fetch_all_form_submissions(form_id)

        form_emails: dict[str, float] = {}  # email -> earliest seen in this form

        for sub in submissions:
            email = _extract_email(sub)
            if not email:
                continue
            submitted_at = sub.get("submittedAt", 0)
            if email not in form_emails or submitted_at < form_emails[email]:
                form_emails[email] = submitted_at

        # Merge into global tracking
        for email, ts in form_emails.items():
            if email not in email_first_seen or ts < email_first_seen[email]:
                email_first_seen[email] = ts

        new_this_week = sum(1 for ts in form_emails.values() if ts >= cutoff_ms)

        form_results.append({
            "name": form_name,
            "count": len(form_emails),
            "new_this_week": new_this_week,
        })

    # Sort forms by count descending for display
    form_results.sort(key=lambda x: x["count"], reverse=True)

    return {
        "forms": form_results,
        "email_first_seen": email_first_seen,  # {email: first_seen_ms}
    }
