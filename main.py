from prefect import flow, task, get_run_logger
from dotenv import load_dotenv
import os
import sys
from shopify_report_main import (
    fetch_all_orders,
    calculate_revenue_and_sales_breakdown,
    render_shopify_report_email_html
)

from vivenu_report_main import (
    fetch_total_transactions,
    fetch_transaction_last_week,
    render_vivenu_weekly_email_html,
    send_outlook_email
)

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
    vivenu_total_transactions = fetch_total_transactions()
    vivenu_week_transactions = fetch_transaction_last_week()
    
    logger.info("Transformation complete.")
    return vivenu_total_transactions, vivenu_week_transactions, shopify_data_processed

@task
def build_html_email(vivenu_total_transactions, vivenu_week_transactions, shopify_data_processed):
    logger = get_run_logger()
    logger.info("Building HTML Email Template...")
    
    shopify_subject, shopify_html_content = render_shopify_report_email_html(shopify_data_processed)
    vivenu_subject, vivenu_html_content = render_vivenu_weekly_email_html(
        total_vivenu_transactions=vivenu_total_transactions,
        week_vivenu_transactions=vivenu_week_transactions
    )
    
    # Combine subjects and HTML bodies
    # For simplicity, let's just use the Vivenu subject and append Shopify's HTML content
    combined_subject = vivenu_subject
    combined_html = vivenu_html_content + "<br>" + shopify_html_content + "<p>Best,<br/>Daniel</p>"
    
    logger.info("Building HTML Email Template complete.")
    return combined_subject, combined_html

@task
def load_data(subject, html_content, recipient_email:str='daniel.delasheras@longislandsc.com'):
    logger = get_run_logger()
    logger.info("Loading data...")
    
    logger.info(f"Loaded data: {subject}")

    send_outlook_email(subject=subject, 
                       html_body=html_content, 
                       recipient=recipient_email)

    logger.info("Load step complete.")


# ======================
# FLOW
# ======================

@flow(name="Weekly Shopify Report Flow")
def weekly_shopify_report_flow():
    logger = get_run_logger()
    logger.info("Flow started.")

    shopify_orders_raw = extract_data()
    vivenu_total_transactions, vivenu_week_transactions, shopify_data_processed = transform_transaction_data(shopify_orders_raw)
    subject, html = build_html_email(vivenu_total_transactions, vivenu_week_transactions, shopify_data_processed)
    load_data(subject=subject, html_content=html)

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
