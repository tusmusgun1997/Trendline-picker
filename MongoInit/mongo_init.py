import pymongo
from dotenv import load_dotenv
import os
load_dotenv()


# Read the MONGO_URL from the .env file
mongo_url = os.getenv('MONGO_URL')
print(mongo_url)
def initialize_mongo():
    client = pymongo.MongoClient(mongo_url)
    db = client["stocks_market"]
    daily_collection = db["daily_stocks_data"]
    weekly_collection = db["weekly_stocks_data"]
    return daily_collection, weekly_collection
