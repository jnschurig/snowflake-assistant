import math
import constants
import pandas as pd
import json

def format_wh_usage(with_header=True, warehouse_type='STANDARD'):
    ''' 
    Returns a block of text, formatted as a table, which contains 
    the warehouse sizes and their credits/hour usage rates.
    example:
    WH Size  | Cr/Hr
    X-Small  | 1    
    Small    | 2    
    Medium   | 4    
    Large    | 8    
    X-Large  | 16   
    2X-Large | 32   
    3X-Large | 64   
    4X-Large | 128  
    5X-Large | 256  
    6X-Large | 512
    '''
    return_val = ''

    wh_sizes = constants.WAREHOUSE_SIZES
    if warehouse_type == 'SNOWPARK-OPTIMIZED':
        wh_sizes = constants.WAREHOUSE_SIZES_SP_OPTIMIZED

    # Determine the correct amount of padding by checking the maximum length of the values.
    if with_header:
        wh_col_header = 'WH Size'
        credit_col_header = 'Cr/Hr'
        size_pad_len = len(wh_col_header)
        credit_pad_len = len(credit_col_header)
    else:
        size_pad_len = 0
        credit_pad_len = 0
    for key in wh_sizes.keys():
        if len(key) > size_pad_len:
            size_pad_len = len(key)

        if len(str(wh_sizes[key]['credit_rate'])) > credit_pad_len:
            credit_pad_len = len(str(wh_sizes[key]['credit_rate']))

    # Create the table
    if with_header:
        return_val = wh_col_header.ljust(size_pad_len) + ' | ' + credit_col_header.ljust(credit_pad_len) + '\n'
    for key in wh_sizes.keys():
        return_val += key.ljust(size_pad_len) + ' | ' + str(wh_sizes[key]['credit_rate']).ljust(credit_pad_len) + '\n'

    return return_val

def format_seconds_interval(seconds=0):
    ''' 
    Takes an integer number of seconds and returns a dict 
    with detail about how that number of seconds can be 
    expressed in written language. Currently it will 
    display hours, minutes, and seconds. It will also 
    return a 'status' key which indicates whether the 
    seconds provided can be used for 'On Demand' or 
    'Always On' warehouse settings.

    Example:
    Input: 3801
    Output: {
        "total_seconds": 3801,
        "status": "On Demand",
        "hours": 1,
        "minutes": 3,
        "seconds": 21,
        "description": "1 hour 3 minutes 21 seconds"
        }
    '''
    seconds = int(seconds)
    return_val = {'total_seconds': seconds}
    if seconds == 0:
        return_val['status'] = 'Always On'
        return_val['description'] = '0 Seconds'
        return_val['hours'] = 0
        return_val['minutes'] = 0
        return_val['seconds'] = 0
    else:
        return_val['status'] = 'On Demand'
        return_val['hours'] = math.floor(seconds/3600)
        seconds = seconds % 3600
        return_val['minutes'] = math.floor(seconds / 60)
        return_val['seconds'] = seconds % 60
        return_val['description'] = ''
        if return_val['hours'] > 0: 
            if return_val['hours'] == 1:
                return_val['description'] += str(return_val['hours']) + ' hour '
            else:
                return_val['description'] += str(return_val['hours']) + ' hours '
        if return_val['minutes'] > 0: 
            if return_val['minutes'] == 1:
                return_val['description'] += str(return_val['minutes']) + ' minute '
            else:
                return_val['description'] += str(return_val['minutes']) + ' minutes '
        if return_val['seconds'] > 0: 
            if return_val['seconds'] == 1:
                return_val['description'] += str(return_val['seconds']) + ' second '
            else:
                return_val['description'] += str(return_val['seconds']) + ' seconds '
        return_val['description'] = return_val['description'].strip()

    return return_val

def convert_list_string(list_or_string, seperator=',', remove_quotes=True):
    ''' 
    Converts a python list object to a formatted string with 
    specified separator, and a python string object into a 
    list object. 
    ''' 
    if type(list_or_string) is list:
        # Convert to a string
        list_or_string = str(list_or_string)
        list_or_string = list_or_string.replace('[', '').replace(']', '').replace(',', seperator)
        if remove_quotes:
            list_or_string = list_or_string.replace("'", '')
    elif type(list_or_string) is str:
        # Convert to a list
        list_or_string = list_or_string.replace(seperator + ' ', seperator).strip()
        list_or_string = list_or_string.split(seperator)

    return list_or_string

def split_dataframe_column_json(input_dataframe, json_column):
    output_dataframe = pd.DataFrame(input_dataframe)

    if json_column in output_dataframe.columns:
        output_dataframe[json_column + '_keys'] = ''
        output_dataframe[json_column + '_values'] = ''
    else:
        return input_dataframe

    for index, row in input_dataframe.iterrows():
        all_keys = []
        all_values = []
        row_json = json.loads(row[json_column])
        for j in row_json:
            all_keys += j.keys()
            all_values += j.values()
        output_dataframe.at[index, json_column + '_keys'] = json.dumps(all_keys)
        output_dataframe.at[index, json_column + '_values'] = json.dumps(all_values)

    return output_dataframe

if __name__ == '__main__':
    pass