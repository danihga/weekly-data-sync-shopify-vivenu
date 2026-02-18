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


    html = f"""
    <h4 style="margin-bottom:4px;"><strong>Merchandise Sales Report Since Launch 2025-11-10</strong></h4>
    <ul>
      <li><strong>Total Revenue:</strong> {fmt_money(total_revenue)}</li>
    </ul>

    <h4 style="margin-bottom:4px;">Sales Breakdown per Item</h4>
    {breakdown_html}
    """
    return subject, html


