import numpy as np
from MongoInit import mongo_init
from helpers.mathService import getLowerHighs, getExtremeHighs, find_second_high

K = 2
order=3


def signal_generator(ticker, result):
    
    close = np.array([])
    dates = np.array([])
    for document in result:
        close = np.append(close, np.log2(document["high"]))
        dates = np.append(dates, document["date"])

    extreme_highs_1 = getLowerHighs(close, order)
    extreme_highs_2 = getExtremeHighs(close, order)
    extreme_lows = extreme_highs_1 + extreme_highs_2
    find_second_high(ticker, extreme_lows, close, 0.025, dates)

daily_collection, weekly_collection = mongo_init.initialize_mongo()
symbols = daily_collection.distinct("symbol")

for i in symbols:
    try:
        print(f"----{i}")
        if i != 'DIVGIITTS':
            result = list(weekly_collection.find({"symbol": i}).sort("date", 1))
            signal_generator(i, result)
    except Exception as e:
        print('---error', e)
        
    
