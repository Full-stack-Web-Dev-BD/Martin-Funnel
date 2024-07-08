import requests
import time
import schedule
import logging
from datetime import datetime, timedelta
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import UK_SCAN_IDS


# Connect to MongoDB
client = MongoClient('mongodb+srv://alamin:1zqbsg2vBlyY1bce@cluster0.sngd13i.mongodb.net/mvp2?retryWrites=true&w=majority')
db = client['mvp2']
UK_Daily_Data = db['UK_Daily_Data']

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

# UK
def add_decimal_point(number, asin):
    if number:
        # Convert the number to a string
        num_str = str(number)

        # Determine the number of digits
        num_digits = len(num_str)

        # Add decimal point before the last two digits
        if num_digits > 2:
            result = float(num_str[:-2] + '.' + num_str[-2:])
        elif num_digits == 2:
            result = float('0.' + num_str)
        else:
            result = float('0.0' + num_str if num_digits == 1 else '0.00')

        result = f"{result:.2f}"
        return result
    else:
        return None

def process_page(page, scanID):
    start_time = time.time()
    data = make_post_request(page,scanID )
    if data:
        data_array = data.get("data", [])
        if not data_array:
            return None, page, 0

        processed_items = []
        for item in data_array:
            asinID = item.get("asin")

            processed_item = {
                "ASIN": item.get("asin"),
                "UK_Buybox_Price": add_decimal_point(item.get("buybox_price"), asin=asinID),
                "UK_Competitive_Sellers": item.get("competitive_sellers"),
                "UK_FBA_Fees": add_decimal_point(item.get("amazon_fees", {}).get("fba_fees"), asin=asinID),
                "UK_Lowest_Price_FBA": add_decimal_point(item.get("lowest_price_new_fba"), asin=asinID),
                "UK_Lowest_Price_FBM": add_decimal_point(item.get("lowest_price_new_fbm"), asin=asinID),
                "UK_FBA_Offers": item.get("new_fba_offers_count"),
                "UK_FBM_Offers": item.get("new_fbm_offers_count"),
                "UK_BSR": item.get("rank"),
                "UK_Referral_Fee": add_decimal_point(item.get("amazon_fees", {}).get("referral_fee"), asin=asinID),
                "UK_Sales_Per_Month": add_decimal_point(item.get("sales_per_month"), asin=asinID),
                "UK_Total_Offers": item.get("total_offers_count"),
                "UK_Units_Per_Month": item.get("units_per_month"),
                "UK_Variable_Closing_Fee": item.get("amazon_fees", {}).get("variable_closing_fee"),
                "UK_Number_Variations": item.get("number_of_variations"),
                "AMZ_Marketplace": item.get("marketplace_id"),
                "UK_Time_Datestamp": datetime.now().isoformat()
            }

            processed_items.append(processed_item)

        if processed_items:
            result = UK_Daily_Data.insert_many(processed_items)
            end_time = time.time()
            return result.inserted_ids[-1], page, end_time - start_time  # Return the last inserted document ID, page number, and time taken

    return None, page, 0

def collect_UK_data(scanID, totalPage):
    print(f"=========Inserting UK Daily Data ======  ScanID, Total Page[{scanID, totalPage}]")

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_page = {executor.submit(process_page, page, scanID): page for page in range(1, totalPage)}

        for future in as_completed(future_to_page):
            page = future_to_page[future]
            try:
                last_inserted_id, page, time_taken = future.result()
                if last_inserted_id:
                    print(f"=============Page {page} Completed inserting in {time_taken:.2f} seconds===== Here is a record ID: {last_inserted_id}")
                else:
                    print(f"No data found or inserted for page: {page}")
            except Exception as exc:
                print(f"Page {page} generated an exception: {exc}")

if __name__ == "__main__":
    for scanDetails in UK_SCAN_IDS :
        collect_UK_data(scanDetails['scanID'],scanDetails['page'])