import numpy as np
from MongoInit.mongo_init import initialize_mongo
from helpers.mathService import getHigherLows, getExtremeLows, find_second_low


K = 2
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


triggered_stocks = []

for i in symbols[1:100]:
    try:
        result = list(weekly_collection.find({"symbol": i}).sort("date", 1))
        print(f"----{i}")
        if i != 'DIVGIITTS':
                signal_generator(i, result)
    except:
        print('---error')
        
