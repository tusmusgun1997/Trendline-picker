import numpy as np
from MongoInit.mongo_init import initialize_mongo
from helpers.mathService import getExtremeLows, getHigherLows, find_second_low

K = 3
order=3

def signal_generator(ticker, result):
    
    close = np.array([])
    dates = np.array([])
    for document in result:
        close = np.append(close, np.log2(document["low"]))
        dates = np.append(dates, document["date"])

    extreme_lows_1 = getHigherLows(close, order)
    extreme_lows_2 = getExtremeLows(close, order)
    extreme_lows = extreme_lows_1 + extreme_lows_2
    find_second_low(ticker, extreme_lows, close, 0.01, dates)
    

daily_collection, weekly_collection = initialize_mongo()
symbols = daily_collection.distinct("symbol")


for i in symbols[:200]:
    try:
        print(i)
        query = {"symbol": i}
        result = daily_collection.find(query)[0]['data']
        signal_generator(i, result)
    except Exception as e: 
        print(e)
