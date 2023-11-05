import pandas as pd
import numpy as np
import pandas as pd
from scipy.signal import argrelextrema
from itertools import combinations
from MongoInit.mongo_init import initialize_mongo


K = 2
order=3
def getExtremeLows(data, order=2):
    '''
    Finds extreme low points in the price pattern, i.e., the lowest lows.
    '''
    # Get lows
    low_idx = argrelextrema(data, np.less, order=order)[0]
    lows = data[low_idx]
    
    # Filter out non-extreme lows
    extreme_lows = []
    for i in range(len(low_idx)):
        if i == 0:
            if data[low_idx[i]] < data[low_idx[i + 1]]:
                extreme_lows.append(low_idx[i])
        elif i == len(low_idx) - 1:
            if data[low_idx[i]] < data[low_idx[i - 1]]:
                extreme_lows.append(low_idx[i])
        else:
            if data[low_idx[i]] < data[low_idx[i - 1]] and data[low_idx[i]] < data[low_idx[i + 1]]:
                extreme_lows.append(low_idx[i])
    
    return extreme_lows
def getHigherLows(data: np.array, order=2):
    '''
    Finds higher low points in the price pattern, i.e., the highest lows.
    '''
    # Get lows
    low_idx = argrelextrema(data, np.less, order=order)[0]
    lows = data[low_idx]

    # Filter out non-extreme higher lows
    higher_lows = []
    for i in range(len(low_idx)):
        if i == 0:
            if data[low_idx[i]] > data[low_idx[i + 1]]:
                higher_lows.append(low_idx[i])
        elif i == len(low_idx) - 1:
            if data[low_idx[i]] > data[low_idx[i - 1]]:
                higher_lows.append(low_idx[i])
        else:
            if data[low_idx[i]] > data[low_idx[i - 1]] and data[low_idx[i]] > data[low_idx[i + 1]]:
                higher_lows.append(low_idx[i])

    return higher_lows
def getExtremeHighs(data, order=2):
    '''
    Finds extreme high points in the price pattern, i.e., the highest highs.
    '''
    # Get highs
    high_idx = argrelextrema(data, np.greater, order=order)[0]
    highs = data[high_idx]
    
    # Filter out non-extreme highs
    extreme_highs = []
    for i in range(len(high_idx)):
        if i == 0:
            if data[high_idx[i]] > data[high_idx[i + 1]]:
                extreme_highs.append(high_idx[i])
        elif i == len(high_idx) - 1:
            if data[high_idx[i]] > data[high_idx[i - 1]]:
                extreme_highs.append(high_idx[i])
        else:
            if data[high_idx[i]] > data[high_idx[i - 1]] and data[high_idx[i]] > data[high_idx[i + 1]]:
                extreme_highs.append(high_idx[i])
    
    return extreme_highs
def getLowerHighs(data, order=2):
    '''
    Finds lower high points in the price pattern, i.e., the lowest highs.
    '''
    # Get highs
    high_idx = argrelextrema(data, np.greater, order=order)[0]
    highs = data[high_idx]

    # Filter out non-extreme lower highs
    lower_highs = []
    for i in range(len(high_idx)):
        if i == 0:
            if data[high_idx[i]] < data[high_idx[i + 1]]:
                lower_highs.append(high_idx[i])
        elif i == len(high_idx) - 1:
            if data[high_idx[i]] < data[high_idx[i - 1]]:
                lower_highs.append(high_idx[i])
        else:
            if data[high_idx[i]] < data[high_idx[i - 1]] and data[high_idx[i]] < data[high_idx[i + 1]]:
                lower_highs.append(high_idx[i])

    return lower_highs

def get_expected_intercept(y1, slope, x1, x2):
    return  y1 - (slope * (x1 - x2))
def get_if_value_within_bounds(lower_value, upper_value, threshold, all_points, is_log_true):
    
    if is_log_true == True:
        upper_value = 2**upper_value
        lower_value = 2**lower_value
    
    
    if(all_points):
        if (upper_value >= lower_value):

            return True
        else:

            return False
    else:
        if (upper_value > lower_value) and (upper_value < (lower_value*(1+threshold))):
            return True
        else:
            return False
def find_second_low(ticker, extreme_lows, close, threshold, dates):
    if not extreme_lows or len(extreme_lows) < 2:
        return False  # If there are no extreme lows or not enough points, return False

    # Iterate through all combinations of two extreme low points
    for index1, index2 in combinations(extreme_lows, 2):
        # Ensure that index1 is the lowest low
        if dates[index1] > dates[index2]:
            index1, index2 = index2, index1

        # Calculate the slope of the line between the two points
        slope = (close[index2] - close[index1]) / (index2 - index1)

        # Calculate the expected intercept at the last date
        expected_intercept = close[index1] + (slope * (len(close) - 1 - index1))
        expected_intercept = get_expected_intercept(close[index1], slope, index1, len(close)-1)


        # Check the conditions
        if get_if_value_within_bounds(expected_intercept, close[-1], threshold, False, True):
#             print(dates[index1], dates[index2])
            points_above = 0
            points_below = 0
            for i in range(index1 + 1, len(close)):
                current_expected_intercept = get_expected_intercept(close[index1], slope, index1, i)
                if not get_if_value_within_bounds(current_expected_intercept, close[i], 0, True, True):
                    points_below += 1
                else: points_above +=1
            ratio_of_points_above = points_above/(points_above+points_below)
            
            if ratio_of_points_above > 0.995:
                if ticker not in triggered_stocks:
                    triggered_stocks.append(ticker)
                print(ticker)
                print("trend_start", dates[index1])
                print("trend_cont", dates[index2])
                print("expected_intercept", expected_intercept)
                print("current_close", close[-1])
                print("-----------")
               

    return False  # No suitable point found

def signal_generator(ticker):
    
    result = list(weekly_collection.find({"symbol": ticker}).sort("date", 1))
    df = pd.DataFrame()
    close_data = []
    dates_data = []

    for document in result:
        close_data.append(np.log2(document["low"]))
#         close_data.append(document["low"])
        dates_data.append(document["date"])


    df["Close"] = close_data
    df["Date"] = dates_data
    df["Date"] = pd.to_datetime(df["Date"])  
    df = df.set_index('Date')
    close = df['Close'].values
    dates = df.index
    extreme_lows_1 = getHigherLows(close, order)
    extreme_lows_2 = getExtremeLows(close, order)
    extreme_lows = extreme_lows_1 + extreme_lows_2
    find_second_low(ticker, extreme_lows, close, 0.025, dates)


daily_collection, weekly_collection = initialize_mongo()
symbols = daily_collection.distinct("symbol")


triggered_stocks = []

for i in symbols[1:100]:
    try:
        print(f"----{i}")
        if i != 'DIVGIITTS':
                signal_generator(i)
    except:
        print('---error')
        
