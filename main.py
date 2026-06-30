from prefect import flow, task, get_run_logger
from dotenv import load_dotenv
import os
import sys
from shopify_report_main import (
    fetch_all_orders,
    calculate_revenue_and_sales_breakdown,
    extract_customer_emails,
    render_shopify_report_email_html
)

from vivenu_report_main import (
    fetch_total_transactions,
    fetch_transaction_last_week,
    render_vivenu_weekly_email_html,
    send_outlook_email
)

from hubspot_report_main import fetch_hubspot_database
from playmetrics_report_main import fetch_playmetrics_contacts
from database_count_report_main import build_database_count, render_database_count_html

# Load environment variables
load_dotenv()


# ======================
# TASKS
# ======================

@task(retries=2, retry_delay_seconds=10)
def extract_data():
    logger = get_run_logger()
    logger.info("Starting data extraction...")
    
    shopify_orders = fetch_all_orders()

    logger.info("Extraction complete.")
    return shopify_orders


@task
def transform_transaction_data(shopify_orders):
    logger = get_run_logger()
    logger.info("Transforming data...")

    shopify_data_processed = calculate_revenue_and_sales_breakdown(shopify_orders)
    shopify_email_data = extract_customer_emails(shopify_orders)
    vivenu_total_transactions = fetch_total_transactions()
    vivenu_week_transactions = fetch_transaction_last_week()

    logger.info("Transformation complete.")
    return vivenu_total_transactions, vivenu_week_transactions, shopify_data_processed, shopify_email_data


@task
def build_database_count_data(shopify_email_data, shopify_data_processed):
    logger = get_run_logger()
    logger.info("Fetching HubSpot database counts...")

    hubspot_data = fetch_hubspot_database()
    playmetrics_data = fetch_playmetrics_contacts()
    db_count = build_database_count(shopify_email_data, hubspot_data, playmetrics_data)

    # Attach revenue for display in the database count section
    from shopify_report_main import fmt_money
    db_count["shopify_revenue"] = fmt_money(shopify_data_processed["total_revenue"])

    logger.info("Database count complete.")
    return db_count

@task
def build_html_email(vivenu_total_transactions, vivenu_week_transactions, shopify_data_processed, db_count):
    logger = get_run_logger()
    logger.info("Building HTML Email Template...")

    shopify_subject, shopify_html_content = render_shopify_report_email_html(shopify_data_processed)
    vivenu_subject, vivenu_html_content = render_vivenu_weekly_email_html(
        total_vivenu_transactions=vivenu_total_transactions,
        week_vivenu_transactions=vivenu_week_transactions
    )
    database_count_html = render_database_count_html(db_count)

    combined_subject = vivenu_subject
    combined_html = f"""
<div style="font-family: Arial, sans-serif; font-size: 14px; color: #333;">

    <p>Hi Team,</p>

    <p>Please find below the latest ticketing, merchandise, and database reports.</p>

    <hr style="border: none; border-top: 1px solid #ddd; margin: 20px 0;" />

    <h3 style="margin-bottom: 5px;">Ticketing Report</h3>
    <div style="margin-bottom: 20px;">
        {vivenu_html_content}
    </div>

    <hr style="border: none; border-top: 1px solid #ddd; margin: 20px 0;" />

    <h3 style="margin-bottom: 5px;">Merchandise Report</h3>
    <div style="margin-bottom: 20px;">
        {shopify_html_content}
    </div>

    <hr style="border: none; border-top: 1px solid #ddd; margin: 20px 0;" />

    <div style="margin-bottom: 20px;">
        {database_count_html}
    </div>

    <p style="margin-top: 30px;">
        Best,<br/>
        Daniel
    </p>

</div>
"""

    logger.info("Building HTML Email Template complete.")
    return combined_subject, combined_html

@task
def load_data(  
        subject, html_content, 
        recipient_email:str='daniel.delasheras@longislandsc.com',
        cc: list[str] | None = None,
        bcc: list[str] | None = None):
    logger = get_run_logger()
    logger.info("Loading data...")
    
    logger.info(f"Loaded data: {subject}")

    send_outlook_email(subject=subject, 
                       html_body=html_content, 
                       recipient=recipient_email,
                       cc=cc, 
                       bcc=bcc)

    logger.info("Load step complete.")


# ======================
# FLOW
# ======================

@flow(name="Weekly Shopify Report Flow")
def weekly_shopify_report_flow():
    logger = get_run_logger()
    logger.info("Flow started.")

    shopify_orders_raw = extract_data()
    vivenu_total_transactions, vivenu_week_transactions, shopify_data_processed, shopify_email_data = transform_transaction_data(shopify_orders_raw)
    db_count = build_database_count_data(shopify_email_data, shopify_data_processed)
    subject, html = build_html_email(vivenu_total_transactions, vivenu_week_transactions, shopify_data_processed, db_count)
    load_data(subject=subject, html_content=html, recipient_email='fred.popp@globallconcepts.com', cc=['daniel.delasheras@longislandsc.com', 'oliver.Whaley@globallconcepts.com', 'travis.lamprecht@theislandfc.com'])

    logger.info("Flow completed successfully.")


# ======================
# ENTRY POINT
# ======================

if __name__ == "__main__":
    try:
        weekly_shopify_report_flow()
    except Exception as e:
        print(f"Flow failed: {e}")
        sys.exit(1)
