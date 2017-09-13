import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta

import pandas_utils


def find_single(train_matrix,test_matrix):
    in_both=list(set(train_matrix.columns)-set(test_matrix.columns))
    in_both=in_both+list(set(test_matrix.columns)-set(train_matrix.columns))
    for k in in_both: 
        if k in train_matrix:
            del train_matrix[k]
        if k in test_matrix:
            del test_matrix[k]
    return train_matrix,test_matrix

def apps_cancelled_before_date(movs,date):
    return movs[(movs['movement_type'].isin(['cancellation'])) & (movs['movement_event_date'] < date)]['application_id'].unique()

def apps_placed_before_date(movs,date):
    return movs[(movs['movement_result'].isin(['ADMITIDO / COLOCADO'])) & (movs['movement_event_date'] < date)]['application_id'].unique()

def apps_exited_before_date(movs,date):
    return np.unique(np.append(apps_cancelled_before_date(movs,date),apps_placed_before_date(movs,date)))

def split_train_test_date(apps_df,movs_df,test_date,train_start_date,test_window):
    #theTimedelta=pd.Timedelta(ltu*365.0/12,unit='d')
    train_end_date=test_date-test_window
    train_apps = pandas_utils.filter_by_time_range(apps_df,'app_start_date',train_start_date,train_end_date).sort_values('app_start_date')
    train_movs=movs_df[movs_df["application_id"].isin(train_apps["table_index"])]
    test_start_date=train_end_date
    test_apps=pandas_utils.filter_by_time_range(apps_df,'app_start_date',test_start_date,test_date).sort_values('app_start_date')
    test_movs=movs_df[movs_df["application_id"].isin(test_apps["table_index"])]
    print "train dates",train_start_date,train_end_date
    print "test dates",test_start_date,test_date
    return train_apps,test_apps,train_movs,test_movs

def split_train_test_apps(apps_df,movs_df,test_date,train_start_date,ltu_length):
    selected_apps = pandas_utils.filter_by_time_range(apps_df,'app_start_date',train_start_date,test_date).sort_values('app_start_date')
    selected_movs = pandas_utils.filter_by_time_range(movs_df,'movement_event_date',train_start_date,test_date).sort_values('movement_event_date')
    exitted = apps_exited_before_date(selected_movs,test_date)
    test_apps = selected_apps[(np.logical_not(selected_apps['table_index'].isin(exitted))) &
                              (pandas_utils.difftime_in_months(test_date,selected_apps['app_start_date']) < ltu_length)]
    test_movs = selected_movs[selected_movs['application_id'].isin(test_apps['table_index'])]
    
    train_apps = selected_apps[(np.logical_not(selected_apps['table_index'].isin(test_apps['table_index'])))]
    train_movs = selected_movs[selected_movs['application_id'].isin(train_apps['table_index'])]
    
    return train_apps,test_apps, train_movs, test_movs

def get_cancellation_date(x):
    return x[x['movement_type'].isin(['cancellation'])].sort_values('movement_event_date').groupby(['application_id']).first().reset_index()[['application_id','movement_event_date']].rename(columns={'movement_event_date': 'cancellation_date'})

def get_placement_date(x):
    return x[x['movement_result'].isin(['ADMITIDO / COLOCADO'])].sort_values('movement_event_date').groupby(['application_id']).first().reset_index()[['application_id','movement_event_date']].rename(columns={'movement_event_date': 'placement_date'})

def get_last_active_date(apps,movs,ref_date):
    apps_cancellations = get_cancellation_date(movs)
    apps_placements = get_placement_date(movs)
    apps_length = apps.merge(apps_cancellations,how='left', left_on='table_index',right_on='application_id').merge(apps_placements,how='left', left_on='table_index',right_on='application_id')[['table_index','app_start_date','cancellation_date','placement_date']]
    apps_length['ref_date'] = ref_date
    apps_length['last_active_date'] = apps_length[['cancellation_date', 'placement_date','ref_date']].min(axis=1)
    
    return apps_length[['table_index','app_start_date','last_active_date']]    

def get_app_length(apps,movs,ref_date=None):
    apps_last_active_date = get_last_active_date(apps,movs,ref_date)
    apps_last_active_date['app_length'] = apps_last_active_date['last_active_date'] - apps_last_active_date['app_start_date']
    
    return apps_last_active_date


def extend_data(apps,movs,labels,ref_date,time_delta):
    if time_delta == None:
        extended_data = apps.merge(labels,on='table_index')[['table_index','app_start_date','ltu']].rename(columns={'table_index':'application_id'})
        extended_data['ref_date'] = ref_date
        extended_data = extended_data[['application_id','app_start_date','ref_date','ltu']]
    else:
        apps_length = get_app_length(apps,movs,ref_date)
        apps_length = apps_length.merge(labels,on='table_index')
        toDataFrame=[]
        for i in xrange(0,apps_length.shape[0]):
            app_id = apps_length['table_index'][i]
            app_st_date = apps_length['app_start_date'][i]
            last_active_date = apps_length['last_active_date'][i]
            app_label = apps_length['ltu'][i]
            count = 0
            curr_ref_date = last_active_date - (int(count)*relativedelta(months=1))
            while (curr_ref_date >= app_st_date):
                toDataFrame.append([app_id,app_st_date,curr_ref_date,app_label])
                count += 1
                curr_ref_date = last_active_date - (int(count)*relativedelta(months=1))
        extended_data = pd.DataFrame(toDataFrame,columns=['application_id','app_start_date','ref_date','ltu'])
    return extended_data


def apps_cancelled_within_n_months(x,n=12):
    return x[(x['movement_type'].isin(['cancellation'])) & (x['months_after_app'] < n)]['application_id'].unique()

def apps_placed_within_n_months(x,n=12):
    return x[(x['movement_result'].isin(['ADMITIDO / COLOCADO'])) & (x['months_after_app'] < n)]['application_id'].unique()


def get_ltu_label_on_date(apps,movs,date,ltu_length):
    apps_movs = pandas_utils.filter_by_time_range(movs[movs['application_id'].isin(apps['table_index'])],'movement_event_date',None,date)
    app_date_dict = dict(zip(apps["table_index"],apps["app_start_date"]))
    apps_movs["app_start_date"] = apps_movs["application_id"].map(app_date_dict)
    apps_movs["months_after_app"] = pandas_utils.difftime_in_months(apps_movs["movement_event_date"],apps_movs["app_start_date"])
    
    print "Generating LTU/Non-LTU labels"
    cancelled_12mo = apps_cancelled_within_n_months(apps_movs,ltu_length)
    placed_12mo = apps_placed_within_n_months(apps_movs,ltu_length)
    non_ltu_apps = np.unique(np.append(cancelled_12mo,placed_12mo))
    
    labelled_apps = apps.copy()
    labelled_apps['ltu'] = np.logical_not(labelled_apps['table_index'].isin(non_ltu_apps))
    
    return labelled_apps[['table_index','ltu']]
