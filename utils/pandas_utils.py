import pandas as pd
import numpy as np


def filter_by_time_range(df,column,start_date,end_date):
    filtered_df = df.copy()
    
    if (start_date != None):
        filtered_df = filtered_df[(filtered_df[column] >= start_date)]
        
    if (end_date != None):
        filtered_df = filtered_df[(filtered_df[column] < end_date)]
    
    return filtered_df

def difftime_in_months(timeA,timeB):
    return (timeA-timeB)/np.timedelta64(1, 'M')
