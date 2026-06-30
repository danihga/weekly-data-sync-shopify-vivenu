import hashlib
import json
import os
from datetime import datetime, timedelta, timezone

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
HASHED_CONTACTS_FILE = os.path.join(DATA_DIR, "hashed-contacts.json")


def _hash_email(email: str) -> str:
    return hashlib.sha256(email.lower().strip().encode()).hexdigest()


def load_hashed_datasets() -> dict:
    """
    Load data/hashed-contacts.json (committed, safe — no raw emails).
    Returns {source_name: list_of_sha256_hashes}.
    """
    if not os.path.exists(HASHED_CONTACTS_FILE):
        return {}
    with open(HASHED_CONTACTS_FILE) as f:
        data = json.load(f)
    return data.get("sources", {})


def build_database_count(shopify_email_data: dict, hubspot_data: dict, playmetrics_data: dict | None = None, days_for_new: int = 7) -> dict:
    """
    Merges Shopify, HubSpot forms, and hashed local datasets into a single
    deduplicated database count.

    Deduplication is done on SHA-256 hashes so that the same email address
    across sources collapses to one entry. Local datasets have no timestamps
    so they count toward the all-time total only (not 'new this week').
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_for_new)
    cutoff_ms = cutoff.timestamp() * 1000

    # hash -> earliest timestamp ms (None = no timestamp known)
    unified: dict[str, float | None] = {}

    # Shopify
    for email, dt in shopify_email_data["email_first_seen"].items():
        h = _hash_email(email)
        ts_ms = dt.timestamp() * 1000
        if h not in unified or (unified[h] is not None and ts_ms < unified[h]):
            unified[h] = ts_ms

    # HubSpot
    for email, ts_ms in hubspot_data["email_first_seen"].items():
        h = _hash_email(email)
        if h not in unified or (unified[h] is not None and ts_ms < unified[h]):
            unified[h] = ts_ms

    # PlayMetrics contacts — no timestamps
    if playmetrics_data:
        for email in playmetrics_data.get("emails", []):
            h = _hash_email(email)
            if h not in unified:
                unified[h] = None

    # Local hashed datasets — no timestamps
    local_sources = load_hashed_datasets()
    for hashes in local_sources.values():
        for h in hashes:
            if h not in unified:
                unified[h] = None

    total_unique = len(unified)
    total_new_this_week = sum(
        1 for ts in unified.values()
        if ts is not None and ts >= cutoff_ms
    )

    local_summary = [
        {"name": name, "count": len(hashes)}
        for name, hashes in local_sources.items()
    ]

    return {
        "shopify_count": shopify_email_data["count"],
        "shopify_new_this_week": shopify_email_data["new_this_week"],
        "hubspot_forms": hubspot_data["forms"],
        "playmetrics_instances": playmetrics_data.get("instances", []) if playmetrics_data else [],
        "local_datasets": local_summary,
        "total_unique": total_unique,
        "total_new_this_week": total_new_this_week,
    }


def _humanize(name: str) -> str:
    if "-" not in name and "_" not in name:
        return name  # already human-readable, preserve casing (e.g. "TIFC Launch Announcement")
    return name.replace("-", " ").replace("_", " ").title()


def render_database_count_html(data: dict) -> str:
    end = datetime.now(timezone.utc).astimezone()
    start = end - timedelta(days=7)
    start_str = start.strftime("%b %d")
    end_str = end.strftime("%b %d")

    total_new = data["total_new_this_week"]
    new_label = f"+{total_new:,} new contact{'s' if total_new != 1 else ''} this week ({start_str}–{end_str}) — Shopify &amp; HubSpot forms"

    # Section header: gray background, acts as visual divider between sources
    SEC  = "padding:7px 8px; font-weight:600; background:#f5f5f5; border-top:1px solid #ddd; border-bottom:1px solid #ddd;"
    SEC_V = "padding:7px 8px; text-align:right; font-weight:600; background:#f5f5f5; border-top:1px solid #ddd; border-bottom:1px solid #ddd;"
    # Sub-item: indented, no border
    SUB  = "padding:5px 8px 5px 20px; color:#555;"
    SUB_V = "padding:5px 8px; text-align:right;"

    hubspot_rows = "".join(
        f'<tr><td style="{SUB}">{f["name"]}</td><td style="{SUB_V}">{f["count"]:,}</td></tr>'
        for f in data["hubspot_forms"]
    )

    pm_rows = ""
    if data.get("playmetrics_instances"):
        pm_rows = f'<tr><td style="{SEC}">PlayMetrics</td><td style="{SEC_V}"></td></tr>'
        for inst in data["playmetrics_instances"]:
            value = '<span style="font-style:italic; color:#aaa; font-size:12px;">WIP — bucket pending</span>' if inst["wip"] else f'{inst["count"]:,}'
            pm_rows += f'<tr><td style="{SUB}">{inst["name"]}</td><td style="{SUB_V}">{value}</td></tr>'

    local_rows = ""
    if data.get("local_datasets"):
        local_rows = f'<tr><td style="{SEC}">Event &amp; Partner Lists</td><td style="{SEC_V}"></td></tr>'
        local_rows += "".join(
            f'<tr><td style="{SUB}">{_humanize(ds["name"])}</td><td style="{SUB_V}">{ds["count"]:,}</td></tr>'
            for ds in data["local_datasets"]
        )

    html = f"""
    <h3 style="margin-bottom:4px;">Database Count</h3>
    <p style="margin:0 0 12px 0; font-size:13px;">{new_label}</p>

    <table style="width:100%; border-collapse:collapse; font-size:14px;">
      <thead>
        <tr style="border-bottom:2px solid #333;">
          <th style="padding:7px 8px; text-align:left;">Source</th>
          <th style="padding:7px 8px; text-align:right;">Unique Contacts</th>
        </tr>
      </thead>
      <tbody>
        <tr><td style="{SEC}">Shopify Customers</td><td style="{SEC_V}">{data['shopify_count']:,}</td></tr>
        <tr><td style="{SEC}">HubSpot Forms</td><td style="{SEC_V}"></td></tr>
        {hubspot_rows}
        {pm_rows}
        {local_rows}
        <tr style="border-top:2px solid #333;">
          <td style="padding:10px 8px; font-weight:700;">Total Unique Contacts</td>
          <td style="padding:10px 8px; text-align:right; font-weight:700;">{data['total_unique']:,}</td>
        </tr>
      </tbody>
    </table>
    <p style="font-size:11px; color:#999; margin-top:6px;">Deduplicated by email across all sources.</p>
    """
    return html
