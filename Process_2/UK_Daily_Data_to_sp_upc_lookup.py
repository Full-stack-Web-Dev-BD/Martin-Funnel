import schedule
import time
from pymongo import MongoClient
from datetime import datetime
import concurrent.futures

# MongoDB connection setup
client = MongoClient('mongodb+srv://alamin:1zqbsg2vBlyY1bce@cluster0.sngd13i.mongodb.net/mvp2?retryWrites=true&w=majority')
db = client['mvp2']
collection = db['UK_Daily_Data']
sp_upc_lookup = db['sp_upc_lookup'] 



# Create index on ASIN field for faster querying
collection.create_index([('ASIN', 1)])

total_udpate_count=[]
# Aggregation pipeline1 to get unique items based on 'ASIN' and include additional fields
pipeline1 = [
    {
        '$group': {
            '_id': '$ASIN',
            'UK_Buybox_Price': {'$first': '$UK_Buybox_Price'},
            'UK_FBA_Fees': {'$first': '$UK_FBA_Fees'},
            'UK_Variable_Closing_Fee': {'$first': '$UK_Variable_Closing_Fee'},
            'UK_Referral_Fee': {'$first': '$UK_Referral_Fee'}
        }
    },
    {
        '$project': {
            '_id': 0,
            'ASIN': '$_id',
            'UK_Buybox_Price': 1,
            'UK_FBA_Fees': 1,
            'UK_Variable_Closing_Fee': 1,
            'UK_Referral_Fee': 1
        }
    }
]


# Function to update a single document
def update_document(item):
    asin = item.get("ASIN")
    update_data = {
        "UK_Buybox_Price_£": item.get('UK_Buybox_Price'),
        "UK_FBA_Fees_£": item.get('UK_FBA_Fees'),
        "UK_Variable_Closing_Fee_£": item.get('UK_Variable_Closing_Fee'),
        "UK_Referral_Fee_£": item.get('UK_Referral_Fee'),
        "time_date_stamp": datetime.now()
    }
    result = sp_upc_lookup.update_many(
        {"asin": asin, "to_be_removed": {"$ne": "Y"}, "gsl_code": "A"},
        {"$set": update_data}
    )
    total_udpate_count.append(asin)
    print(f"{len(total_udpate_count)} Updated {result.matched_count} documents for ASIN: {asin}")




def collect_Data_to_Sp_Gsl_lookup2():
  print("Collecting Data...")
  try:
      # Perform the aggregation
      result = list(collection.aggregate(pipeline1))
      print(f"Total unique ASINs fetched: {len(result)}")

      # Using ThreadPoolExecutor for concurrent updates
      with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
          # Submitting update tasks for each item in result
          futures = [executor.submit(update_document, item) for item in result]

          # Wait for all futures to complete
          concurrent.futures.wait(futures)

  except Exception as e:
      print(f"An error occurred: {e}")
      
      
# Aggregation pipeline2 to calculate UK Profit and update documents
pipeline2 = [
    {
        '$match': {
            'to_be_removed': {'$ne': 'Y'},
            'gsl_code': 'A',
            'UK_Buybox_Price_£': {'$exists': True, '$type': 'double'},
            'UK_FBA_Fees_£': {'$exists': True, '$type': 'double'},
            'UK_Variable_Closing_Fee_£': {'$exists': True, '$type': 'double'},
            'UK_Referral_Fee_£': {'$exists': True, '$type': 'double'},
            'seller_price': {'$exists': True, '$type': 'double'}
        }
    },
    {
        '$addFields': {
            'UK_Profit': {
                '$round': [
                    {
                        '$subtract': [
                            '$UK_Buybox_Price_£',
                            {
                                '$sum': [
                                    '$UK_FBA_Fees_£',
                                    '$UK_Variable_Closing_Fee_£',
                                    '$UK_Referral_Fee_£',
                                    '$seller_price'
                                ]
                            }
                        ]
                    },
                    2  # Rounding to 2 decimal places
                ]
            }
        }
    },
    {
        '$out': 'sp_upc_lookup'  # Output the results back to the same collection
    }
]


def calculate_Sp_gsl_Lookup2_UK_Profit():
  print("Calculating UK Profit ...")
  try:
      # Perform the aggregation and update
      result = sp_upc_lookup.aggregate(pipeline2)
      
      print("UK Profit calculation and update completed.")
      
  except Exception as e:
      print(f"An error occurred during aggregation or update: {e}")




print("WIll run at Scedule")

def job():
    print("Running scheduled tasks...")
    collect_Data_to_Sp_Gsl_lookup2()
    calculate_Sp_gsl_Lookup2_UK_Profit()

# Schedule the job to run every day at 14:00
schedule.every().day.at("04:10").do(job)

# Run the scheduler indefinitely
while True:
    schedule.run_pending()
    time.sleep(60)  # Check every minute

