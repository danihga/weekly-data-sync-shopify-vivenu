import pandas as pd
import os
import shopify
from dotenv import load_dotenv
from datetime import datetime, timezone
import os
from datetime import datetime, timedelta, timezone
from msal import ConfidentialClientApplication
import time
import requests

load_dotenv()

# ==============================
# 0) Config: load secrets
# ==============================
SHOPIFY_API_KEY = os.getenv("SHOPIFY_API_KEY")
SHOPIFY_PASSWORD = os.getenv("SHOPIFY_PASSWORD")
SHOPIFY_SHOP_NAME = os.getenv("SHOPIFY_SHOP_NAME")
API_VERSION_ENV = os.getenv("API_VERSION") # Use a different variable name to avoid confusion



# ==============================
# 1️⃣ Setup Shopify Connection
# ==============================
def setup_shopify_session():
    """
    Initializes and activates the Shopify API session.
    """
    shop_url = f"https://{SHOPIFY_API_KEY}:{SHOPIFY_PASSWORD}@{SHOPIFY_SHOP_NAME}.myshopify.com/admin"
    shopify.ShopifyResource.set_site(shop_url)
    shopify.ShopifyResource.set_version(API_VERSION_ENV)
    print("Shopify session setup complete.")


# ==============================
# 2️⃣ Fetch all orders since a given date
# ==============================
def fetch_all_orders(since_date="2025-01-01T00:00:00Z"):
    """
    Fetch all orders from the Shopify API since a specific date.
    Handles pagination to retrieve all orders.
    """
    setup_shopify_session()
    all_orders = []
    page = shopify.Order.find(created_at_min=since_date, status='any', limit=250)
    
    while page:
        all_orders.extend(page)
        # The `next_page()` method is part of the paginated resource object
        # It returns a new page if it exists, otherwise it returns None
        # We need to add a small delay to avoid hitting the API rate limit.
        time.sleep(1) 
        try:
            page = page.next_page()
        except Exception as e:
            print(f"No more pages or an error occurred: {e}")
            break
            
    if not all_orders:
        print("⚠️ No order data returned.")
    
    return all_orders


# ==============================
# 3️⃣ Calculate revenue and sales breakdown
# ==============================
def calculate_revenue_and_sales_breakdown(orders):
    """
    Calculates total revenue and a breakdown of sales per item from a list of orders.
    """
    if not orders:
        return 0, pd.DataFrame()

    total_revenue = 0
    sales_breakdown = {}

    for order in orders:
        total_revenue += float(order.total_price)
        for item in order.line_items:
            product_title = item.title
            if product_title in sales_breakdown:
                sales_breakdown[product_title] += item.quantity
            else:
                sales_breakdown[product_title] = item.quantity

    # Convert the sales breakdown to a DataFrame for easier rendering
    breakdown_df = pd.DataFrame(
        list(sales_breakdown.items()), columns=["Product", "Quantity Sold"]
    )
    breakdown_df = breakdown_df.sort_values(by="Quantity Sold", ascending=False)
    
    return {'total_revenue':total_revenue, 'breakdown_df':breakdown_df}


# ==============================
# 4️⃣ Render Email
# ==============================
def fmt_money(x):
    try:
        return f"${float(x):,.2f}"
    except (ValueError, TypeError):
        return str(x)


def extract_customer_emails(orders, days_for_new: int = 7) -> dict:
    """
    Extract unique customer emails from all orders.
    Returns all-time unique emails with their earliest order timestamp,
    for use in cross-source deduplication.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_for_new)
    email_first_seen: dict[str, datetime] = {}

    for order in orders:
        email = getattr(order, "email", None)
        if not email:
            continue
        email = email.lower().strip()
        if not email:
            continue

        created_at_str = getattr(order, "created_at", None)
        if not created_at_str:
            continue

        try:
            order_dt = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            continue

        if email not in email_first_seen or order_dt < email_first_seen[email]:
            email_first_seen[email] = order_dt

    new_this_week = sum(1 for dt in email_first_seen.values() if dt >= cutoff)

    return {
        "email_first_seen": email_first_seen,  # {email: first_order_datetime}
        "count": len(email_first_seen),
        "new_this_week": new_this_week,
    }


def render_shopify_report_email_html(
    shopify_orders, since_date="January 1, 2025"
):
    """
    Renders the Shopify report into an HTML email body.
    """
    today_str = datetime.now(timezone.utc).astimezone().strftime("%B %d, %Y")
    subject = f"Shopify Revenue Report - {today_str}"

    total_revenue = shopify_orders['total_revenue']
    sales_breakdown_df = shopify_orders['breakdown_df']

    # Convert DataFrame to HTML table
    breakdown_html = sales_breakdown_df.to_html(index=False, border=0, classes="sales-table")
    breakdown_html = breakdown_html.replace('<table border="0" class="dataframe sales-table">', '<table style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">')
    breakdown_html = breakdown_html.replace('<th>', '<th style="background-color:#f2f2f2; padding: 8px; text-align: left; border: 1px solid #ddd;">')
    breakdown_html = breakdown_html.replace('<td>', '<td style="padding: 8px; text-align: left; border: 1px solid #ddd;">')


    SEC   = "padding:7px 8px; font-weight:600; background:#f5f5f5; border-top:1px solid #ddd; border-bottom:1px solid #ddd;"
    SEC_V = "padding:7px 8px; text-align:right; font-weight:600; background:#f5f5f5; border-top:1px solid #ddd; border-bottom:1px solid #ddd;"
    SUB   = "padding:5px 8px 5px 20px; color:#555;"
    SUB_V = "padding:5px 8px; text-align:right;"

    breakdown_rows = "".join(
        f'<tr><td style="{SUB}">{row["Product"]}</td><td style="{SUB_V}">{int(row["Quantity Sold"]):,}</td></tr>'
        for _, row in sales_breakdown_df.iterrows()
    )

    html = f"""
    <h3 style="margin-bottom:4px;">Merchandise Report</h3>
    <p style="margin:0 0 12px 0; font-size:13px;">Since launch — Nov 10, 2025</p>

    <table style="width:100%; border-collapse:collapse; font-size:14px;">
      <thead>
        <tr style="border-bottom:2px solid #333;">
          <th style="padding:7px 8px; text-align:left;">Product</th>
          <th style="padding:7px 8px; text-align:right;">Units Sold</th>
        </tr>
      </thead>
      <tbody>
        <tr><td style="{SEC}">Total Revenue</td><td style="{SEC_V}">{fmt_money(total_revenue)}</td></tr>
        {breakdown_rows}
      </tbody>
    </table>
    """
    return subject, html


