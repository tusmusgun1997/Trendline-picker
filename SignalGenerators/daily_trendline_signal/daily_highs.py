import numpy as np
from MongoInit.mongo_init import initialize_mongo
from helpers.mathService import getExtremeHighs, getLowerHighs, find_second_high

K = 3
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
    find_second_high(ticker, extreme_lows, close, 0.01, dates)
    

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