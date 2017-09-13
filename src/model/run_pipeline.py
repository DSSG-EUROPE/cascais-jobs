import json
import sys
import pandas as pd
sys.path.append("../../")
from utils import model_utils
from utils import db_utils
from utils import feature_utils
from utils import iefp_data_utils
from utils import train_test_utils
import os
import datetime as dt
#Static Variables
MIN_NUM_ARGS = 2
LABEL_COLUMN = 'ltu'

#Functions

def printUsage():
    print "python run_pipeline.py <config-filepath>"

#Check Script Arguments
if len(sys.argv) < MIN_NUM_ARGS:
    print "Wrong Usage!"
    printUsage()
    exit(1)

#Read Variables
config_filepath = sys.argv[1]
#feature_matrix_output_filepath = sys.argv[2]

with open(config_filepath) as data_file:    
    config = json.load(data_file)

print "Reading applications and movements tables from DB"
conn = db_utils.connect_to_db()
apps,movs = iefp_data_utils.get_clean_data(conn)      

print "generating global system and historical dataframes"
system_info = feature_utils.generate_system_info(apps)
historical_limit = config['historical'][0]['limit']
historical_info = feature_utils.generate_historical(apps, historical_limit)


print "Running pipeline"
train_test_id_counter = 1

for time_split in config['time_splits']:
    model_id_counter = 1
    
    split_type = time_split['type']
    action_date = pd.to_datetime(time_split['action_date'])
    train_timedelta = pd.Timedelta(time_split['train_timedelta'])
    test_window_size = pd.Timedelta(time_split['test_window_size'])
    train_st_date = action_date - train_timedelta
    
    print "Splitting train/test apps and movements"
    ltu_length = config['labels'][0]['ltu_length']
    if split_type == "action":
        train_apps,test_apps,train_movs,test_movs = train_test_utils.split_train_test_apps(apps,movs,action_date,train_st_date,ltu_length) 
        print "Generating LTU Labels"
        train_labels = train_test_utils.get_ltu_label_on_date(train_apps,movs,action_date,ltu_length)
        test_labels = train_test_utils.get_ltu_label_on_date(test_apps,movs,action_date + test_window_size,ltu_length)
        print "Extending data"
        extended_train = train_test_utils.extend_data(train_apps,movs,train_labels,action_date,pd.Timedelta('30D'))
        extended_test = train_test_utils.extend_data(test_apps,movs,test_labels,action_date,None)
    elif split_type == "date":
        train_apps,test_apps,train_movs,test_movs = train_test_utils.split_train_test_date(apps,movs,action_date,train_st_date,test_window_size)
        train_labels = train_test_utils.get_ltu_label_on_date(train_apps,movs,action_date,ltu_length)
        #use end time as today. Which applications we use for testing is determined by the window, but we can check the outcome of the application at any time after. 
        test_labels = train_test_utils.get_ltu_label_on_date(test_apps,movs,dt.date.today(),ltu_length)
        extended_train = train_test_utils.extend_data(train_apps,movs,train_labels,action_date,None)
        extended_test = train_test_utils.extend_data(test_apps,movs,test_labels,action_date,None)
    
    for feature_set in config['feature_sets']:
        feature_set_list = feature_set['list']
        if split_type=="date" and "dynamic" in feature_set_list:
            print "Warning: not possible to use dynamic features when predicting on application date"
            continue
        print "Generating Features"
        pd.options.mode.chained_assignment = None #turns off warning for chained assignment
        train_matrix = feature_utils.generate_matrix(extended_train,train_apps,train_movs,feature_set_list,system_info,historical_info)
        test_matrix = feature_utils.generate_matrix(extended_test,test_apps,test_movs,feature_set_list, system_info, historical_info)
        #remove columns that do not appear in both train and test matrixes (e.g., infrequent categorical variables)
        train_matrix,test_matrix=train_test_utils.find_single(train_matrix,test_matrix)
        for model in config['models']:
            model_id = model_utils.get_time()+'_'+model['type']+'_'+str(len(feature_set_list))+'fsets'
            print "Training Model"
            model_obj = model_utils.train_model(model['type'],train_matrix,{})
            if model_obj==None:
                continue
        for model in config['models']:
            model_params = model['params']
            model_params_index = str(config['models'].index(model))
            model_id = str(model_id_counter)+'_'+model['type']+'_paramset'+model_params_index+'_'+feature_set['name']+'_'+config_filepath
            train_test_id = str(train_test_id_counter)+'_'+model_utils.get_time()

            print "Training Model"
            model_obj = model_utils.train_model(model['type'],train_matrix,model_params)
            
            print "Testing Model"
            model_results = model_utils.test_model(model['type'],model_obj,test_matrix,model_id)
            print "Evaluating Model Performance"
            model_res_apps = model_utils.prepare_model_results_for_analysis(model_results,apps,model_id)
            test_subsets = model_utils.build_test_eval_subset_df(config['data_subsets'],model_res_apps)
            model_performance = model_utils.evaluate_model(model_res_apps, config['cutoffs'], test_subsets, model_id,train_test_id)

            print "Appending performance metrics to db"
            db_utils.write_table(model_performance,conn, 'model_output', 'performances', if_exists='append')
               
            print "Getting config details"
            train_total = len(train_matrix)
            train_proportion_ltu = train_matrix.ltu.mean()
            test_total = len(test_matrix)
            test_proportion_ltu = test_matrix.ltu.mean()
            features_string = " ".join(str(x) for x in feature_set_list)
            params_string = '_'.join('{}{}'.format(key, val) for key, val in model_params.items())
            
            model_config = pd.DataFrame({'model_id':model_id,'model_type':model['type'],'params':params_string, 'feature_set':features_string, "experiment":config_filepath}, index=[0])
            model_config = model_config.reindex(columns=['model_id','model_type','params','feature_set','experiment'])
            
            train_test_config = pd.DataFrame({'model_id':model_id, 'train_test_id':train_test_id,'split_type':split_type, 'action_date':action_date,'train_timedelta': time_split['train_timedelta'], 'train_total':train_total,'train_proportion_ltu':train_proportion_ltu,'test_total':test_total,'test_proportion_ltu':test_proportion_ltu}, index=[0])
            train_test_config = train_test_config.reindex(columns=['model_id', 'train_test_id', 'split_type','action_date','train_timedelta','train_total','train_proportion_ltu','test_total','test_proportion_ltu'])
            
            print "Getting feature importances"
            feature_importance = model_utils.get_feature_importances(model_obj, model['type'], model_id, train_test_id, train_matrix, top_n=10)
            print "Appending model config, train_test_config, and feature importances to db tables"
            db_utils.write_table(model_config,conn, 'model_output', 'model_configs',if_exists='append')
            db_utils.write_table(train_test_config,conn, 'model_output', 'train_test_configs', if_exists='append')
            db_utils.write_table(feature_importance,conn,'model_output','feature_importances',if_exists='append')
           
            model_id_counter += 1
            train_test_id_counter += 1
