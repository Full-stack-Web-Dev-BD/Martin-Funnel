import requests
import schedule
import time
from datetime import datetime
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import US_SCAN_IDS

# Connect to MongoDB
client = MongoClient('mongodb+srv://alamin:1zqbsg2vBlyY1bce@cluster0.sngd13i.mongodb.net/mvp2?retryWrites=true&w=majority')
db = client['mvp2']
US_Daily_Data = db['US_Daily_Data']

# Configuration
API_KEY = "3440|CbZEnbJHKBFRadAjSWk94KiSSvwGu5WHBbG4hw0lea0c94cc"

# Headers
headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

# Function to make the POST request
def make_post_request(page, scanID):
    API_URL = f"https://app.rocketsource.io/api/v3/scans/{scanID}"
    body = {
        "per_page": 100,
        "page": page
    }
    try:
        response = requests.post(API_URL, headers=headers, json=body)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")

# Conversion and processing functions
def calculate_US_conversion(arg):
    _usdToGbp = 0.79
    if isinstance(arg, (int, float)):
        return arg * _usdToGbp
    else:
        return None

def round_to_two_decimals(value):
    if isinstance(value, (int, float)):
        return round(value / 100, 2)
    else:
        return None

def process_page(page, scanID):
    data = make_post_request(page, scanID)
    if data:
        data_array = data.get("data", [])
        if not data_array:
            return None, page

        processed_items = []
        for item in data_array:
            processed_item = {
                "ASIN": item.get("asin"),
                "US_BSR_Percentage": round_to_two_decimals(item.get("bsr_percentage")),
                "US_Buybox_Price": calculate_US_conversion(round_to_two_decimals(item.get("buybox_price"))),
                "US_Competitive_Sellers": item.get("competitive_sellers"),
                "US_FBA_Fees": calculate_US_conversion(round_to_two_decimals(item.get("amazon_fees", {}).get("fba_fees"))),
                "US_Lowest_Price_FBA": calculate_US_conversion(round_to_two_decimals(item.get("lowest_price_new_fba"))),
                "US_Lowest_Price_FBM": calculate_US_conversion(round_to_two_decimals(item.get("lowest_price_new_fbm"))),
                "US_FBA_Offers": item.get("new_fba_offers_count"),
                "US_FBM_Offers": item.get("new_fbm_offers_count"),
                "US_BSR": item.get("rank"),
                "US_Referral_Fee": calculate_US_conversion(round_to_two_decimals(item.get("amazon_fees", {}).get("referral_fee"))),
                "US_Sales_Per_Month": round_to_two_decimals(item.get("sales_per_month")),
                "US_Total_Offers": item.get("total_offers_count"),
                "US_Units_Per_Month": item.get("units_per_month"),
                "US_Variable_Closing_Fee": calculate_US_conversion(round_to_two_decimals(item.get("amazon_fees", {}).get("variable_closing_fee"))),
                "US_Number_Variations": item.get("number_of_variations"),
                "AMZ_Marketplace": item.get("marketplace_id"),
                "US_Time_Datestamp": datetime.now().isoformat(),
            }
            processed_items.append(processed_item)

        if processed_items:
            result = US_Daily_Data.insert_many(processed_items)
            return result.inserted_ids[-1], page  # Return the last inserted document ID and page number

    return None, page

def collect_US_data(scanID, totalPage):
    print(f"====================Inserting US Daily Data ======wit scanID , Page[{scanID, totalPage}]")

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_page = {executor.submit(process_page, page, scanID): page for page in range(1, totalPage)}

        for future in as_completed(future_to_page):
            page = future_to_page[future]
            start_time = time.time()
            try:
                last_inserted_id, page = future.result()
                end_time = time.time()
                time_taken = end_time - start_time
                if last_inserted_id:
                    print(f"=============Page {page} Completed inserting in {time_taken:.2f} seconds===== Here is a record ID: {last_inserted_id}")
                else:
                    print(f"No data found or inserted for page: {page}")
            except Exception as exc:
                print(f"Page {page} generated an exception: {exc}")

def insert_us_data():
    for scanDetails in US_SCAN_IDS:
        collect_US_data(scanDetails['scanID'],scanDetails['page'])
        
        
print("WIll run at Scedule")

def job():
    print("Running scheduled tasks...")
    insert_us_data()

# Schedule the job to run every day at 14:00
schedule.every().day.at("04:20").do(job)

# Run the scheduler indefinitely
while True:
    schedule.run_pending()
    time.sleep(60)  # Check every minute
    