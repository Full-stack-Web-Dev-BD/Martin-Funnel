from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb+srv://alamin:1zqbsg2vBlyY1bce@cluster0.sngd13i.mongodb.net/mvp2?retryWrites=true&w=majority')
db = client['mvp2']

# Define the destination collection
A2A_Asin_Lookup = db['A2A_Asin_Lookup']

# Define GroupA and GroupB collections
groupA_collections_s = ['sp_upc_lookup', 'sp_upc_lookup2', 'sp_gsl_lookup2', 'sp_ID_lookup']
groupA_collections_a = ['UK_daily_data']
groupB_collection = db['website_collection']

def migrate_data():
    try:
        # Get all ASINs from GroupB
        groupB_asins = set(doc['ASIN'] for doc in groupB_collection.find({}, {'ASIN': 1}))
        print(groupB_collection)
        # Process collections with TYPE 'S'
        for collection_name in groupA_collections_s:
            collection = db[collection_name]
            for doc in collection.find():
                asin = doc.get('ASIN')
                if asin and asin not in groupB_asins:
                    # Prepare the document for the destination collection
                    dest_doc = {
                        "ASIN": asin,
                        "TYPE": 'S',
                        "seller_name": doc.get('seller_name'),
                        "seller_price": doc.get('seller_price'),
                        "Supplier_code": doc.get('Supplier_code')
                    }
                    # Insert into the destination collection
                    A2A_Asin_Lookup.insert_one(dest_doc)
                    print(f"Inserted ASIN {asin} from {collection_name} into destination collection with TYPE 'S'.")
        
        # Process collections with TYPE 'A'
        for collection_name in groupA_collections_a:
            collection = db[collection_name]
            for doc in collection.find():
                asin = doc.get('ASIN')
                if asin:
                    # Check if ASIN exists in any GroupA (TYPE 'S') or GroupB (website_collection)
                    asin_exists = False
                    for groupA_collection_name in groupA_collections_s:
                        if db[groupA_collection_name].find_one({"ASIN": asin}):
                            asin_exists = True
                            break
                    if not asin_exists and asin not in groupB_asins:
                        # Prepare the document for the destination collection
                        dest_doc = {
                            "ASIN": asin,
                            "TYPE": 'A',
                            "seller_name": doc.get('amazon.co.uk'),
                            "seller_price": doc.get('UK_Buybox_Price'),
                            "Supplier_code": "https://www.amazon.co.uk/dp/"+asin
                        }
                        # Insert into the destination collection
                        A2A_Asin_Lookup.insert_one(dest_doc)
                        print(f"Inserted ASIN {asin} from {collection_name} into destination collection with TYPE 'A'.")
    
    except Exception as e:
        print(f"An error occurred: {e}")

# Call the migrate_data function
migrate_data()
