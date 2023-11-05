from scipy.signal import argrelextrema
import numpy as np
from itertools import combinations


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

def find_second_high(ticker, extreme_highs, close, threshold, dates):
    if not extreme_highs or len(extreme_highs) < 2:
        return False  # If there are no extreme highs or not enough points, return False

    # Iterate through all combinations of two extreme high points
    for index1, index2 in combinations(extreme_highs, 2):
        # Ensure that index1 is the highest high
        if dates[index1] > dates[index2]:
            index1, index2 = index2, index1

        # Calculate the slope of the line between the two points
        slope = (close[index2] - close[index1]) / (index2 - index1)

        # Calculate the expected intercept at the last date
        expected_intercept = get_expected_intercept(close[index1], slope, index1, len(close) - 1)

        # Check the conditions (opposite of find_second_low)
        if slope < 0 and get_if_value_within_bounds( close[-1], expected_intercept, threshold, False, True) and index2 < len(dates)-15:
            points_above = 0
            points_below = 0
#             print(slope)
            for i in range(index1 + 1, len(close)):
                current_expected_intercept = get_expected_intercept(close[index1], slope, index1, i)
#                 print(dates[index1], dates[i], dates[index2], 2**current_expected_intercept, dates[i])
                if current_expected_intercept < close[i]:
                    points_above += 1
                else:
                    points_below += 1
            ratio_of_points_below = points_below / (points_above + points_below)
            if ratio_of_points_below > 0.99:
                print(ticker)
                print("trend_start", dates[index1])
                print("trend_cont", dates[index2])
                print("expected_intercept", 2**expected_intercept)
                print("current_close", 2**close[-1])
                print("-----------")

    return False  # No suitable point found

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

        if get_if_value_within_bounds(expected_intercept, close[-1], threshold, False, True):
            points_above = 0
            points_below = 0
            for i in range(index1 + 1, len(close)):
                current_expected_intercept = get_expected_intercept(close[index1], slope, index1, i)
                if current_expected_intercept < close[i]:
                    points_above += 1
                else:
                    points_below += 1
            ratio_of_points_above = points_above/(points_above+points_below)
            
            if ratio_of_points_above == 1:
                if slope < 0:
                    print(ticker)
                    print("trend_start", dates[index1])
                    print("trend_cont", dates[index2])
                    print("expected_intercept", expected_intercept)
                    print("current_close", close[-1])
                    print("-----------")
               

    return False  # No suitable point found
