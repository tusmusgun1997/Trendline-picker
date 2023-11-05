from requests import Session
import json
from datetime import datetime, timedelta
import pymongo
from datetime import datetime
import pandas as pd
import time
import numpy as np
from requests import Session
from MongoInit.mongo_init import initialize_mongo




# Function to fetch data for a specific date range
def fetch_data(symbol, from_date, to_date):
    # Get the cookies from the main page (will update automatically in headers)
    s.get("https://www.nseindia.com/")
    # Format the date strings
    from_date_str = from_date.strftime("%d-%m-%Y")
    to_date_str = to_date.strftime("%d-%m-%Y")
    
    # Get the API data
    response = s.get(f"https://www.nseindia.com/api/historical/cm/equity?symbol={symbol}&series=[%22EQ%22]&from={from_date_str}&to={to_date_str}")
    if response.status_code == 200:
        data = json.loads(response.text)
        return data
    else:
        print(f"Failed to fetch data for {from_date_str} to {to_date_str}. Status Code: {response.status_code}")
        return None


def string_to_datetime(date_string, date_format="%Y-%m-%d"):
    try:
        date_obj = datetime.strptime(date_string, date_format)
        return date_obj
    except ValueError:
        # Handle an invalid date format
        return None


def update_or_insert_weekly_data(symbol, new_daily_data, weekly_collection):
    # Fetch existing weekly data for the symbol and sort it by dates
    existing_weekly_data = list(weekly_collection.find({"symbol": symbol}).sort("date", 1))

    if existing_weekly_data:
        # Get the highest date from the existing weekly data
        highest_date = existing_weekly_data[-1]["date"]

        # Determine the Sunday of the week for the highest date
        highest_date_obj = datetime.strptime(highest_date, "%Y-%m-%d")
        sunday_of_week = highest_date_obj + timedelta(days=(6 - highest_date_obj.weekday()))

        # Check if the new data's date falls within the same week as the Sunday
        new_data_date = datetime.strptime(new_daily_data["date"], "%Y-%m-%d")
        if new_data_date <= sunday_of_week:
            week_data = existing_weekly_data[-1]
            week_data["low"] = min(week_data["low"], new_daily_data["low"])
            week_data["high"] = max(week_data["high"], new_daily_data["high"])
            week_data["close"] = new_data["close"]
            weekly_collection.replace_one({"_id": week_data["_id"]}, week_data)
        else:
            # Create a new entry for the new week
            new_week_data = {
                "symbol": symbol,
                "date": new_daily_data["date"],
                "open": new_daily_data["open"],
                "low": new_daily_data["low"],
                "high": new_daily_data["high"],
                "close": new_daily_data["close"],
            }
            weekly_collection.insert_one(new_week_data)


daily_collection, weekly_collection = initialize_mongo()
symbols = daily_collection.distinct("symbol")


s = Session()
s.headers.update({"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36"})

# Should be in ascending
dates_to_be_added = [{"year": 2023, "month": 11, "day": 3}] 
for symb in symbols:
    print(symb)
    query = {"symbol": symb}
    result = daily_collection.find(query)
    
    url_symb = symb.replace('&', '%26')
    
    existing_data = []
    _id = ""
    for document in result:
        if 'data' in document:
            existing_data = document['data']
            _id = document["_id"]
            
    
    for date in dates_to_be_added:
        start_date = datetime(date["year"], date["month"], date["day"])
        end_date = datetime(date["year"], date["month"], date["day"])
    
        data = fetch_data(url_symb, start_date, end_date)
        for i in data['data']:
            new_data = {
                "open": i['CH_OPENING_PRICE'],
                "close": i['CH_CLOSING_PRICE'],
                "high": i['CH_TRADE_HIGH_PRICE'],
                "low": i['CH_TRADE_LOW_PRICE'],
                "date": i['CH_TIMESTAMP']
            }
            existing_data.append(new_data)
            print(new_data)
            daily_collection.update_one(
                    {"_id": _id},  # You may need to replace "_id" with the actual unique identifier field in your documents
                    {"$set": {"data": existing_data}}
            )
            update_or_insert_weekly_data(symb, new_data, weekly_collection)
    
            
            