# TIFC Weekly Report — data-sync

## What This Is

A Prefect flow that runs every Monday at 5pm UTC via GitHub Actions and sends a weekly email to Fred Popp (CC: Daniel, Oliver, Travis) with three sections:

1. **Ticketing Report** — Vivenu all-time + last 7 days
2. **Merchandise Report** — Shopify all-time since Nov 10 2025 launch
3. **Database Count** — deduplicated unique contacts across all sources

## File Map

| File | Purpose |
|---|---|
| `main.py` | Prefect flow — orchestrates tasks, sends email |
| `vivenu_report_main.py` | Vivenu API fetch + render |
| `shopify_report_main.py` | Shopify fetch, revenue calc, email extraction, render |
| `hubspot_report_main.py` | HubSpot form submissions (8 TIFC forms, paginated) |
| `playmetrics_report_main.py` | PlayMetrics contacts via GCS bucket CSV |
| `database_count_report_main.py` | Cross-source deduplication + render |
| `hash_contacts.py` | **Local-only** — hashes raw CSV/Excel email lists into `data/hashed-contacts.json` |

## Database Count — How It Works

Sources:
- **Shopify** — unique customer emails from all orders (has timestamps → counts toward "new this week")
- **HubSpot** — submissions from 8 TIFC forms (has timestamps → counts toward "new this week")
- **PlayMetrics TIFC East** — `all_player_contacts.csv` from GCS bucket `club1811-reports-playmetrics-prod` (no timestamps — all-time total only)
- **PlayMetrics TIFC West** — WIP, awaiting bucket ID from PlayMetrics
- **Event & Partner Lists** — SHA-256 hashed emails in `data/hashed-contacts.json` (no timestamps — all-time total only)

Deduplication is SHA-256 hash-based so the same email across sources counts once. Raw emails never touch the repo.

### Adding a new event/partner list

1. Drop the CSV or Excel file in `data/` (gitignored)
2. Run: `.venv/bin/python3.11 hash_contacts.py`
3. Enter a human-readable name when prompted (e.g. `TIFC Launch Announcement`)
4. Commit `data/hashed-contacts.json` — the raw file stays local

## Email Recipients

- **To:** `fred.popp@globallconcepts.com`
- **CC:** `daniel.delasheras@longislandsc.com`, `oliver.Whaley@globallconcepts.com`, `travis.lamprecht@theislandfc.com`

## Environment Variables

Required in `.env` (local) and GitHub Actions secrets (CI):

| Variable | Where |
|---|---|
| `VIVENU_API_KEY` | Vivenu |
| `SHOPIFY_API_KEY`, `SHOPIFY_PASSWORD`, `SHOPIFY_SHOP_NAME`, `SHOPIFY_API_SECRET` | Shopify |
| `OUTLOOK_CLIENT_ID`, `OUTLOOK_TENANT_ID`, `OUTLOOK_CLIENT_SECRET`, `OUTLOOK_USER_EMAIL` | Microsoft Graph (email sending) |
| `HUBSPOT_API_KEY` | HubSpot Service Key (`pat-na1-...`) |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to GCS service account JSON (locally: `~/.config/gcloud/gc-data-platform-sa.json`; in CI: written to `/tmp/gcs-key.json` from `GCS_SERVICE_ACCOUNT_KEY` secret) |
| `PM_BUCKET_TIFC_EAST` | `club1811-reports-playmetrics-prod` |
| `PM_BUCKET_TIFC_WEST` | Empty for now — add when PlayMetrics provides the bucket |

## Running Locally

```bash
cd data-sync
source .venv/bin/activate
python main.py
```

## GitHub Actions

Runs every Monday 5pm UTC (`0 17 * * 1`). Can also be triggered manually:
**Actions → Weekly Vivenu Report → Run workflow**

The GCS key is stored as the full JSON content in the `GCS_SERVICE_ACCOUNT_KEY` secret and written to `/tmp/gcs-key.json` at runtime.

## Table Styling Convention

Both Merchandise and Database Count tables follow the same pattern:
- **Gray row** (`background:#f5f5f5`) = section header / key summary — visual divider
- **White indented rows** = sub-items, no individual borders
- **2px solid `#333`** = column header bottom border and total row top border only
