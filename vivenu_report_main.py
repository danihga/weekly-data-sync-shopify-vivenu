import pandas as pd
import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
import os
from datetime import datetime, timedelta, timezone
from msal import ConfidentialClientApplication

load_dotenv()

VIVENU_API_KEY = os.getenv("TEST")
headers = {
    "Authorization": f"Bearer {VIVENU_API_KEY}",
    "Accept": "application/json"
}

# ==============================
# 1️⃣ Fetch all transactions
# ==============================
def fetch_transaction_volume() -> pd.DataFrame:
    """
    Fetch all transactions from the Vivenu API and return as a DataFrame.
    """
    url = "https://vivenu.com/api/transactions"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    data = response.json()
    df = pd.DataFrame(data.get("docs", []))
    
    if df.empty:
        print("⚠️ No transaction data returned.")
    
    return df


# ==============================
# 2️⃣ Count premium / reserved tickets
# ==============================
def fetch_premium_reserved_tickets(transactions: pd.DataFrame) -> tuple[int, int]:
    """
    Count how many premium and reserved tickets exist in the given transaction DataFrame.
    """
    premium = 0
    reserved = 0
    
    # Defensive check
    if "tickets" not in transactions.columns:
        return premium, reserved
    
    for tickets in transactions["tickets"]:
        if not isinstance(tickets, list) or len(tickets) == 0:
            continue
        
        ticket_id = tickets[0].get("ticketTypeId")
        if ticket_id == "68dffb2d8649a8638f79d42e":
            premium += 1
        elif ticket_id == "68dffb3d8649a8638f79d42f":
            reserved += 1
    
    return premium, reserved

def fetch_number_orders(transactions):
    number_ticket_purchases = transactions["tickets"].apply(lambda x: len(x) if isinstance(x, list) else 0).sum()
    return number_ticket_purchases

# ==============================
# 3️⃣ Get overall totals
# ==============================
def fetch_total_transactions() -> tuple[float, int, int, int]:
    """
    Compute total revenue, number of tickets purchased,
    and counts of premium/reserved tickets.
    """
    transactions = fetch_transaction_volume()

    transactions = transactions[transactions['status'] == 'COMPLETE']
    
    if transactions.empty:
        return 0, 0, 0, 0
    
    total_revenue = transactions["realPrice"].sum()
    
    premium, reserved = fetch_premium_reserved_tickets(transactions)
    number_ticket_purchases = fetch_number_orders(transactions)
    
    return total_revenue, number_ticket_purchases, premium, reserved


# ==============================
# 4️⃣ Get transactions in the last N days
# ==============================
def fetch_transaction_last_week(days: int = 7) -> tuple[float, str, int, int]:
    """
    Compute revenue and completion rate over the last `days` days.
    """
    transactions = fetch_transaction_volume()
    
    if "createdAt" not in transactions.columns:
        print("⚠️ 'createdAt' column missing from API response.")
        return 0, "N/A", 0, 0
    
    transactions["createdAt"] = pd.to_datetime(transactions["createdAt"], errors="coerce")
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    cutoff_transactions = transactions[transactions["createdAt"] > cutoff]
    
    # Revenue in last `days`
    sum_revenue = cutoff_transactions["realPrice"].sum()
    
    # ✅ Use Vivenu's actual status values: COMPLETE / CANCELED
    completed = cutoff_transactions[cutoff_transactions["status"] == "COMPLETE"].shape[0]
    canceled = cutoff_transactions[cutoff_transactions["status"] == "CANCELED"].shape[0]
    
    if completed + canceled > 0:
        completion = f"{int((completed / (completed + canceled)) * 100)}%"
    else:
        completion = "N/A"
    
    # Premium / reserved for completed transactions
    completed_transactions = cutoff_transactions[cutoff_transactions["status"] == "COMPLETE"]
    premium, reserved = fetch_premium_reserved_tickets(completed_transactions)

    number_ticket_purchases = fetch_number_orders(cutoff_transactions)
    
    return sum_revenue, completion, premium, reserved, number_ticket_purchases

def fmt_money(x): 
    return f"${x:,.2f}"

def render_vivenu_weekly_email(
    revenue_total, total_num_orders, total_premium, total_reserved,
    revenue_week, completions_week, premium_week, reserved_week, orders_week,
    since_date="2025-11-10"
):
    end = datetime.now(timezone.utc).astimezone()  # local time display
    start = end - timedelta(days=7)
    start_str = start.strftime("%b %d")
    end_str = end.strftime("%b %d")

    subject = f"Vivenu Weekly Report ({start_str}–{end_str})"
    body = f"""Hi Fred,

Here’s the weekly Vivenu report.

Membership Report Since Launch {since_date}
• Total Transaction Volume: {fmt_money(revenue_total)}
• Online Sales: {total_num_orders:,}
• Premium Deposits: {total_premium:,}
• Reserved Deposits: {total_reserved:,}

Weekly Report ({start_str}–{end_str})
• Transaction Volume: {fmt_money(revenue_week)}
• Completion Ratio: {completions_week}
• Transactions: {orders_week:,} total
• Increase in Deposits: {premium_week:,} Premium, {reserved_week:,} Reserved

If you’d like, I can break this down by event or sales channel next.

Best,
Daniel
"""
    return subject, body

revenue_total, total_num_orders, total_premium, total_reserved = fetch_total_transactions()
revenue_week, completions_week, premium_week,  reserved_week, orders_week = fetch_transaction_last_week()

# Example usage with your variables:
subject, body = render_vivenu_weekly_email(
    revenue_total, total_num_orders, total_premium, total_reserved,
    revenue_week, completions_week, premium_week, reserved_week, orders_week
)

# -----------------------------
# 0) Config: load secrets
# -----------------------------
load_dotenv()

CLIENT_ID     = os.getenv("OUTLOOK_CLIENT_ID")
TENANT_ID     = os.getenv("OUTLOOK_TENANT_ID")
CLIENT_SECRET = os.getenv("OUTLOOK_CLIENT_SECRET")
SENDER_EMAIL  = os.getenv("OUTLOOK_USER_EMAIL")  # the Outlook mailbox to send from

# -----------------------------
# 1) Helpers to format the report
# -----------------------------
def fmt_money(x):
    try:
        return f"${float(x):,.2f}"
    except Exception:
        return str(x)

def render_vivenu_weekly_email_html(
    revenue_total, total_num_orders, total_premium, total_reserved,
    revenue_week, completions_week, premium_week, reserved_week, orders_week,
    since_date="2025-11-10"
):
    end = datetime.now(timezone.utc).astimezone()
    start = end - timedelta(days=7)
    start_str = start.strftime("%b %d")
    end_str = end.strftime("%b %d")
    subject = f"Vivenu Weekly Report ({start_str}–{end_str})"

    html = f"""
    <p>Hi Fred,</p>
    <p>Here’s the weekly Vivenu report.</p>

    <h4 style="margin-bottom:4px;">Membership Report Since Launch {since_date}</h4>
    <ul>
      <li><strong>Total Transaction Volume:</strong> {fmt_money(revenue_total)}</li>
      <li><strong>Online Sales:</strong> {int(total_num_orders):,}</li>
      <li><strong>Premium Deposits:</strong> {int(total_premium):,}</li>
      <li><strong>Reserved Deposits:</strong> {int(total_reserved):,}</li>
    </ul>

    <h4 style="margin-bottom:4px;">Weekly Report ({start_str}–{end_str})</h4>
    <ul>
      <li><strong>Transaction Volume:</strong> {fmt_money(revenue_week)}</li>
      <li><strong>Completion Ratio:</strong> {completions_week}</li>
      <li><strong>Transactions:</strong> {int(orders_week):,} total</li>
      <li><strong>Increase in Deposits:</strong> {int(premium_week):,} Premium, {int(reserved_week):,} Reserved</li>
    </ul>

    <p>Best,<br/>Daniel</p>
    """
    return subject, html

# -----------------------------
# 2) Send email via Microsoft Graph
#    (Application permission: Mail.Send)
# -----------------------------
def send_outlook_email(subject: str, html_body: str, recipient: str):
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    app = ConfidentialClientApplication(
        client_id=CLIENT_ID,
        authority=authority,
        client_credential=CLIENT_SECRET,
    )

    token = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    if "access_token" not in token:
        raise RuntimeError(f"Auth failed: {token.get('error_description', token)}")

    # With application permissions, use /users/{user-id-or-email}/sendMail
    endpoint = f"https://graph.microsoft.com/v1.0/users/{SENDER_EMAIL}/sendMail"
    payload = {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": html_body},
            "toRecipients": [{"emailAddress": {"address": recipient}}],
        },
        "saveToSentItems": True,
    }

    resp = requests.post(
        endpoint,
        headers={"Authorization": f"Bearer {token['access_token']}",
                 "Content-Type": "application/json"},
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    print(f"✅ Email sent to {recipient}")

if __name__ == "__main__":
    # These variables should already come from your earlier code:
    # revenue_total, total_num_orders, total_premium, total_reserved = fetch_total_transactions()
    # revenue_week, completions_week, premium_week, reserved_week, orders_week = fetch_transaction_last_week()

    # For example, assume they are defined in your runtime:
    subject, html = render_vivenu_weekly_email_html(
        revenue_total, total_num_orders, total_premium, total_reserved,
        revenue_week, completions_week, premium_week, reserved_week, orders_week
    )

    # Send to Gmail recipient
    send_outlook_email(subject, html, recipient="danidhg00@gmail.com")