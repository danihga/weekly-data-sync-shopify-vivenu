"""
Run this locally whenever you add a new contact list to data/.

  python hash_contacts.py

Reads every CSV/Excel in data/, hashes the email column with SHA-256,
and writes data/hashed-contacts.json. The JSON is safe to commit —
hashes are irreversible. Raw files stay gitignored and never leave this machine.
"""

import glob
import hashlib
import json
import os
from datetime import datetime, timezone

import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
OUTPUT_FILE = os.path.join(DATA_DIR, "hashed-contacts.json")


def hash_email(email: str) -> str:
    normalized = email.lower().strip()
    return hashlib.sha256(normalized.encode()).hexdigest()


def main():
    patterns = [
        os.path.join(DATA_DIR, "*.csv"),
        os.path.join(DATA_DIR, "*.xlsx"),
        os.path.join(DATA_DIR, "*.xls"),
    ]
    files = []
    for pattern in patterns:
        files.extend(glob.glob(pattern))

    if not files:
        print("No CSV/Excel files found in data/ — nothing to hash.")
        return

    sources = {}
    for filepath in sorted(files):
        name = os.path.splitext(os.path.basename(filepath))[0]
        try:
            df = pd.read_csv(filepath, dtype=str) if filepath.endswith(".csv") else pd.read_excel(filepath, dtype=str)
            email_col = next((c for c in df.columns if "email" in c.lower()), None)
            if email_col is None:
                print(f"  ⚠️  No email column in {name}, skipping.")
                continue

            emails = df[email_col].dropna().str.lower().str.strip()
            emails = emails[emails != ""].unique().tolist()
            hashes = [hash_email(e) for e in emails]
            sources[name] = hashes
            print(f"  ✅ {name}: {len(hashes)} emails hashed")
        except Exception as e:
            print(f"  ⚠️  Could not read {name}: {e}")

    # Merge with any existing hashes so previous lists aren't lost
    existing = {}
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE) as f:
            existing = json.load(f).get("sources", {})

    merged = {**existing, **sources}  # new run overwrites same-named sources

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": merged,
    }
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    total = sum(len(v) for v in merged.values())
    print(f"\nWrote {OUTPUT_FILE}")
    print(f"Total hashed entries across {len(merged)} source(s): {total}")
    print("Safe to commit — no raw emails in this file.")


if __name__ == "__main__":
    main()
