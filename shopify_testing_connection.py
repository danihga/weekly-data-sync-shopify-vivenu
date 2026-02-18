import os
import requests
from dotenv import load_dotenv

load_dotenv()  # loads .env values if present

STORE = os.getenv("STORE")         # "the-island-fc-store.myshopify.com"
SHOPIFY_API_KEY = os.getenv("SHOPIFY_API_KEY")
SHOPIFY_API_SECRET = os.getenv("SHOPIFY_API_SECRET")

def test_shopify_connection():
    if not (STORE and SHOPIFY_API_KEY and SHOPIFY_API_SECRET):
        raise RuntimeError("❌ Missing Shopify env vars. Set SHOPIFY_STORE_DOMAIN, SHOPIFY_API_KEY, SHOPIFY_API_PASSWORD")

    url = f"https://{STORE}.myshopify.com"

    print(f"🔍 Testing Shopify API connection to: {url}")

    try:
        response = requests.get(url, auth=(SHOPIFY_API_KEY, SHOPIFY_API_SECRET), timeout=20)
        print("🔍 Status Code:", response.status_code)

        if response.status_code == 200:
            data = response.json()
            shop_name = data.get("shop", {}).get("name", "Unknown")
            print("✅ Connected successfully!")
            print(f"🏪 Store Name: {shop_name}")
            print("📦 Raw Response:")
            print(data)
        else:
            print("❌ Shopify API returned an error:")
            print(response.text)

    except Exception as e:
        print("❌ Exception during request:", e)


if __name__ == "__main__":
    test_shopify_connection()