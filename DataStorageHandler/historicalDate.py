from requests import Session
import json
from datetime import datetime, timedelta
import pymongo
from datetime import datetime
import pandas as pd
import time
import numpy as np
import yfinance as yf
import pandas as pd
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
    
def find_index_by_date(data, target_date):
    for index, record in enumerate(data):
        if record['date'] == target_date:
            return index
    return -1  

def group_data_into_weeks(symbol,data):
    weekly_data = []
    current_week_start = None
    current_week_data = {
        "open": None,
        "low": None,
        "high": None,
        "close": None,
    }

    current_weekday = None

    for daily_entry in data:
        date = datetime.strptime(daily_entry["date"], "%Y-%m-%d")
        weekday = date.weekday()

        if current_week_start is None or weekday < current_weekday:
            # A new week begins, or it's the first day of data
            if current_week_start:
                weekly_data.append({
                    "symbol": symbol,
                    "date": current_week_start,
                    "open": current_week_data["open"] if current_week_data["open"] is not None else 0,
                    "low": current_week_data["low"] if current_week_data["low"] is not None else 0,
                    "high": current_week_data["high"] if current_week_data["high"] is not None else 0,
                    "close": current_week_data["close"] if current_week_data["close"] is not None else 0,
                })
            current_week_start = daily_entry["date"]
            current_week_data = {
                "open": daily_entry["open"],
                "low": daily_entry["low"],
                "high": daily_entry["high"],
                "close": daily_entry["close"],
            }

        else:
            # Continue with the current week
            current_week_data["close"] = daily_entry["close"]
            if current_week_data["open"] is not None:
                current_week_data["open"] = min(current_week_data["open"], daily_entry["open"])
            else:
                current_week_data["open"] = daily_entry["open"]
            if current_week_data["low"] is not None:
                current_week_data["low"] = min(current_week_data["low"], daily_entry["low"])
            else:
                current_week_data["low"] = daily_entry["low"]
            if current_week_data["high"] is not None:
                current_week_data["high"] = max(current_week_data["high"], daily_entry["high"])
            else:
                current_week_data["high"] = daily_entry["high"]

        current_weekday = weekday

    if current_week_start:
        weekly_data.append({
            "symbol": symbol,
            "date": current_week_start,
            "open": current_week_data["open"] if current_week_data["open"] is not None else 0,
            "low": current_week_data["low"] if current_week_data["low"] is not None else 0,
            "high": current_week_data["high"] if current_week_data["high"] is not None else 0,
            "close": current_week_data["close"] if current_week_data["close"] is not None else 0,
        })

    return weekly_data



df = pd.read_excel("./tickers_name.xlsx")
pd.set_option('display.max_rows' , None)
df = df.dropna()
df = df[df['Market capitalization as on March 31, 2023\n(Rs in Lakhs)'] != '*Not available for trading as on March 31, 2023']
symbols = df['Symbol'].values



daily_collection, weekly_collection = initialize_mongo()

s = Session()
s.headers.update({"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36"})



for symb in symbols:
    url_symb = symb
    if '&' in symb:
        url_symb = symb.replace('&', '%26')
    historical_data_list = []
    start_date = datetime(2023, 10, 31)
    end_date = datetime(2023, 10, 31)
    print(url_symb)

    while True:
        data = fetch_data(url_symb, start_date, end_date)
        if data:
            for i in data['data']:
                historical_data_list.append({
                    "open": i['CH_OPENING_PRICE'],
                    "close": i['CH_CLOSING_PRICE'],
                    "high": i['CH_TRADE_HIGH_PRICE'],
                    "low": i['CH_TRADE_LOW_PRICE'],
                    "date": i['CH_TIMESTAMP']
                })
            if(len(data["data"]) == 0): break
        last_date_received = historical_data_list[-1]["date"]
        end_date = string_to_datetime(last_date_received,"%Y-%m-%d") - timedelta(days=1)  # Set end_date to the day before the start_date
        start_date = end_date - timedelta(days=365)  # Set start_date to one year before the new end_date
        print(start_date, end_date)
        time.sleep(0.001)

    historical_data_list.reverse()
    filtered_historical_data_list = [entry for entry in historical_data_list if all(entry[key] != 0 for key in ["open", "high", "low", "close"])]

    stock = yf.Ticker(f"{symb}.NS")
    split_history = stock.splits
    yf_data = pd.DataFrame(split_history)
    yf_data = yf_data.reset_index()
    print(yf_data)
    split_data_dict = yf_data.rename(columns={'Date': 'Date', 'Stock Splits': 'split'}).to_dict(orient='records')
    split_data = [{'ex_date': str(item['Date'].date()), 'split': round(item['split'], 2)} for item in split_data_dict]
    split_data.sort(key=lambda x: x['ex_date'])
    
    for record in split_data:
        ex_date = record['ex_date']
        split = record['split']

        idx = find_index_by_date(filtered_historical_data_list, ex_date)

        if idx != -1:
            for i in range(0, idx):
                filtered_historical_data_list[i]['open'] = round(filtered_historical_data_list[i]['open'] / split, 2)
                filtered_historical_data_list[i]['high'] = round(filtered_historical_data_list[i]['high'] / split, 2)
                filtered_historical_data_list[i]['low'] = round(filtered_historical_data_list[i]['low'] / split, 2)
                filtered_historical_data_list[i]['close'] = round(filtered_historical_data_list[i]['close'] / split, 2)



    schema = {
        "symbol": symb,
        "data": filtered_historical_data_list
    }
    print(schema)
    daily_collection.insert_one(schema)

for symbol in symbols:
    print(symbol)
    daily_data = daily_collection.find({"symbol": symbol})[0]['data']
    weekly_data = group_data_into_weeks(symbol,daily_data)

    # Insert the weekly data into the weekly collection
    if weekly_data:
        weekly_collection.insert_many(weekly_data)