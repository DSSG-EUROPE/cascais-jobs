import pandas as pd
import numpy as np
import sys
sys.path.append("../../")
from utils import db_utils


#Function to Read tables
def get_model_output():
    conn = db_utils.connect_to_db()
    performances = db_utils.read_table(conn, 'model_output', 'performances')

    model_configs = db_utils.read_table(conn, 'model_output', 'model_configs')
    train_test_configs = db_utils.read_table(conn, 'model_output', 'train_test_configs')
    feature_importances = db_utils.read_table(conn, 'model_output', 'feature_importances')
    joined_perf = pd.merge(performances, model_configs.drop_duplicates(), how='left', on = 'model_id', copy=False)
    
    joined_perf = pd.merge(joined_perf, train_test_configs, how = 'left', on = ['model_id', 'train_test_id'])

    return model_configs, train_test_configs, performances, feature_importances, joined_perf

def filter_output(joined_perf, filter_dict):
    filtered_output = joined_perf.copy()
    for key in filter_dict.keys():
        filtered_output = filtered_output[filtered_output[key].isin(filter_dict[key])]
    return filtered_output

#Functions to summarize merics
def summary_across_action_dates(experiment_output):
    counts = pd.DataFrame(experiment_output.groupby(['model_id','feature_set','train_timedelta']).value.count())/len(experiment_output.metric.unique())
    counts.columns = ['count']
    medians = experiment_output.groupby(['model_id','feature_set','train_timedelta','metric']).value.median().unstack()
    medians.columns = ['precision_median','recall_median']
    std_devs = experiment_output.groupby(['model_id','feature_set','train_timedelta','metric']).value.std().unstack()
    std_devs.columns = ['precision_std', 'recall_std']
    
    perf_table = pd.merge(medians, std_devs, left_index = True, right_index=True)
    perf_table = pd.merge(perf_table, counts, left_index = True, right_index=True)
    perf_table.sort_values(by=['precision_median'], ascending = False, inplace=True)
    perf_table.reindex_axis(sorted(perf_table.columns), axis=1)
    return perf_table

def metric_by_action_date(experiment_output, metric):
    results = experiment_output[experiment_output['metric']==metric]
    results = results.groupby(['model_id','feature_set','train_timedelta','action_date']).value.max().unstack()
    return results
