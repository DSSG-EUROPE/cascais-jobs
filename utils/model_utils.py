import pandas as pd
import numpy as np
import pickle
import statsmodels.api as sm
import time
import datetime
import sklearn
from sklearn import svm
from sklearn.neural_network import MLPClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from sklearn.tree import DecisionTreeClassifier

import pandas_utils


#Create model id from time stamp
def get_time():
    return str(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H:%M:%S'))

##MODEL TRAINING FUNCTIONS
def svm(params):
    return sklearn.svm.SVC(**params) #default params: probability=True

def random_forest(params):
    return sklearn.ensemble.RandomForestClassifier(random_state=4321, **params) # always set your random seed for reproducibility!

def dec_tree(params):
    return sklearn.tree.DecisionTreeClassifier(**params)

def knn(params):
    return sklearn.neighbors.KNeighborsClassifier(**params)

def ann(params):
    return sklearn.neural_network.MLPClassifier(**params)

def adaboost():
    return sklearn.ensemble.AdaBoostClassifier()
    
def get_model(model_type):
    return sklearn.ensemble.AdaBoostClassifier(**params)

def adaboost(params):
    return sklearn.ensemble.AdaBoostClassifier(**params)

def get_model(model_type, params):

    switcher = {
        'rf': random_forest,
        'svm': svm,
        'knn': knn,
        'ann': ann,
        'tree': dec_tree,
        'adaboost': adaboost,
    }
    # Get the function from switcher dictionary
    func = switcher.get(model_type)
    # Execute the function
    return func(params)

#Train Generic Model
def train_model(model_type,train_matrix,model_params):
    y = train_matrix.ltu.astype(float)
    X = train_matrix.drop(['ltu'], 1)
    if model_type=="lg":
        try:
            logit = sm.Logit(y,X.astype(float))
            model = logit.fit_regularized(**model_params)
        except:
            model=None
    else:
        model = None
        model = get_model(model_type,model_params)
        #Define Model input/output 
        model.fit(X=X, y=y)
    return model
def test_model(model_type,model,test_matrix,model_id):
    if model_type=="lg":
        pred = model.predict(test_matrix.drop(['ltu'],1))
        model_results = pd.Series(index=test_matrix.index, data=pred)
    else:
        #print test_matrix.drop(['ltu'],1).columns
        pred = model.predict_proba(test_matrix.drop(['ltu'],1))
        model_results = pd.Series(index=test_matrix.index, data=pred[:,1])
    labels = pd.DataFrame(test_matrix['ltu'])
    all_model_results = pd.concat([labels,pd.DataFrame(data=model_results,columns=[model_id])], axis=1)
    return all_model_results

 #Train Random Forest Model
def train_rf(train_matrix):
    #Define Model input/output
    y = train_matrix.ltu.astype(float)
    X = train_matrix.drop(['ltu'], 1)

    model = sklearn.ensemble.RandomForestClassifier(n_estimators=100,
                       criterion='gini',
                       max_depth=None,
                       random_state=4321) # always set your random seed for reproducibility!
    model.fit(X=X, y=y)
    return model
 
def train_lg(train_matrix):
    y = train_matrix.ltu.astype(float)
    X = train_matrix.drop(['ltu'], 1)
    #X["intercept"]=1.0
    logit = sm.Logit(y,X.astype(float))
    result = logit.fit(maxiter=1000)
    return result
#Train Generic Model
def train(train_matrix, model_type, model_params):
    #@param model_params dictionary 
    #Define Model input/output
    y = train_matrix.ltu.astype(float)
    X = train_matrix.drop(['ltu'], 1)
    
    n_estimators = model_params[n_estimators]
    model = sklearn.ensemble.model_type(**model_params)# always set your random seed for reproducibility!
    model.fit(X=X, y=y)
    return model
   
#Read model from file
def read_model(model_filepath):
    return pickle.load((open(model_filepath, 'rb')))
    

#Write model to file
def write_model(model,model_filepath):
    pickle.dump(model, open(model_filepath, 'wb'))

#Uses the model to predict on the test data
def predict_rf(model,test_matrix):
    return model.predict_proba(X=test_matrix.drop(['ltu'],1))

#

def predict_lg(model,test_matrix):
    return model.predict(test_matrix.drop(['ltu'],axis=1))

def prepare_model_results_for_analysis(model_res,applications,model_id):
    model_results_apps = model_res.reset_index().rename(columns = {model_id : 'prob'}).merge(applications, left_on='application_id',right_on='table_index')
    model_results_apps['months_in_system'] =  np.floor(pandas_utils.difftime_in_months(model_results_apps['ref_date'],model_results_apps['app_start_date']))
    model_results_apps = model_results_apps[['application_id','months_in_system','prob','ltu']]
    
    return model_results_apps

def build_test_eval_subset_df(subsets_config,model_res_apps):
    #Calculate month bucket LTU prop
    ltu_prop_by_month = model_res_apps.groupby(['months_in_system']).ltu.mean().reset_index(name='ltu_prop')
    ltu_prop_overall = model_res_apps.ltu.mean()
    
    #Filter months in test set based on subset type and value
    test_subsets = []
    
    for subset_config in subsets_config:
        subset_type = subset_config['type']
        subset_cutoff = subset_config['cutoff']
    
        if (subset_type == 'month_thresh'):
            test_subsets.append({'subset_type':subset_type, 'subset_cutoff': subset_cutoff, 'month_thresh':subset_cutoff})
        elif (subset_type == 'ltu_prop_weight'):
            month_thresh = ltu_prop_by_month[ltu_prop_by_month['ltu_prop'] < ltu_prop_overall*subset_cutoff]['months_in_system'].max()
            test_subsets.append({'subset_type':subset_type, 'subset_cutoff': subset_cutoff, 'month_thresh':month_thresh})
        else:
            print 'Warning: Invalid subset type:', subset_type
        
    return pd.DataFrame(test_subsets).reindex(columns = ['subset_type','subset_cutoff','month_thresh'])

def model_metrics(pred,ltu_actual,cutoff_k):
    pred_dict = {'prob': pred,
                'actual': ltu_actual}
    pred_df = pd.DataFrame(pred_dict, index=ltu_actual.index)
    pred_df.sort_values('prob',ascending=False,inplace=True)
    pred_df['pred'] = (([True] * cutoff_k) + ([False] * (pred_df.shape[0]-cutoff_k)))

    true_positive = float(pred_df[(pred_df['pred']) & (pred_df['actual'])].shape[0])
    false_positive = float(pred_df[(pred_df['pred']) & (np.logical_not(pred_df['actual']))].shape[0])
    true_negative = float(pred_df[(np.logical_not(pred_df['pred'])) & (np.logical_not(pred_df['actual']))].shape[0])
    false_negative = float(pred_df[(np.logical_not(pred_df['pred'])) & (pred_df['actual'])].shape[0])
    precision = true_positive/(true_positive + false_positive)
    recall = true_positive/(true_positive + false_negative)
    accuracy = (true_positive+true_negative)/(true_positive+true_negative+false_positive+false_negative)
    #roc_auc_score = sklearn.metrics.roc_auc_score(pred_df['actual'], pred_df['pred'])
    #total=len(pred_df)
    ltu=true_positive+false_negative
    #proportion_ltu=ltu*1.0/total
    return (true_positive,true_negative,false_positive,false_negative,precision,recall,accuracy)


def evaluate_model(model_results, k_cutoffs, test_subsets, model_id, train_test_id):
    
    model_performance = pd.DataFrame(columns=['model_id','train_test_id','k_value','k_type','metric','subset_type','subset_cutoff','value'])

    print "Calculating performance metrics for model: "+model_id +", train_test_split: "+train_test_id

    model_performances_list = []
    
    for index, row in test_subsets.iterrows():
        subset_type = row['subset_type']
        subset_cutoff = row['subset_cutoff']
        month_threshold = row['month_thresh']
        
        for k in k_cutoffs:
            k_type = k['type']
            k_value = k['value']

            if k_type == "number":
                cutoff_number = k_value
            else:
                cutoff_number = 1 #placeholder; want to replace with function to convert a percentage to a number k
            model_results_subset = model_results[model_results['months_in_system'] <= month_threshold]
            subset_ltu_prop = model_results_subset.ltu.mean()
            subset_size = model_results_subset.shape[0]
            tp,tn,fp,fn,precision,recall,accuracy=model_metrics(model_results_subset.loc[:, 'prob'], model_results_subset.loc[:, 'ltu'], cutoff_number)
            
            model_performances_list.append({'model_id':model_id,'train_test_id':train_test_id, 'k_value':k_value, 'k_type':k_type, 'metric':'precision', 'subset_type': subset_type, 'subset_cutoff': subset_cutoff, 'month_threshold': month_threshold,'subset_size': subset_size, 'subset_ltu_prop': subset_ltu_prop, 'value':precision})
            model_performances_list.append({'model_id':model_id, 'train_test_id':train_test_id, 'k_value':k_value, 'k_type':k_type, 'metric':'recall', 'subset_type': subset_type, 'subset_cutoff': subset_cutoff, 'month_threshold': month_threshold,'subset_size': subset_size, 'subset_ltu_prop': subset_ltu_prop, 'value':recall})
            model_performances_list.append({'model_id':model_id, 'train_test_id':train_test_id, 'k_value':k_value, 'k_type':k_type, 'metric':'accuracy', 'subset_type': subset_type, 'subset_cutoff': subset_cutoff, 'month_threshold': month_threshold,'subset_size': subset_size, 'subset_ltu_prop': subset_ltu_prop, 'value':accuracy})

    model_performance = pd.DataFrame(model_performances_list)
    model_performance = model_performance.reindex(columns = ['model_id','train_test_id','k_value','k_type','metric','subset_type','subset_cutoff','month_threshold','subset_size','subset_ltu_prop','value'])
        
    return model_performance.reset_index(drop=True)


## FEATURE IMPORTANCE FUNCTIONS
def get_feature_importances(model_obj, model_type, model_id, train_test_id, train_data, top_n=10):
    feature_names = train_data.drop(['ltu'],1).columns

    if model_type == 'rf':
        return pd.DataFrame(zip([model_id]*len(feature_names), [train_test_id]*len(feature_names), feature_names, model_obj.feature_importances_),\
                         columns = ['model_id','train_test_id', 'feature', 'importance']).\
                         sort_values(by='importance',ascending=False).head(n=top_n)
        
    elif model_type=='lg':
        resultsData=pd.DataFrame(columns=["model_id","feature","importance"])
        resultsData["model_id"]=[model_id]*len(feature_names)
        resultsData["train_test_id"]=[train_test_id]*len(feature_names)
        resultsData["feature"]=model_obj.pvalues.index
        resultsData["importance"]=np.round(model_obj.pvalues.values,2)
        resultsData.replace([np.inf, -np.inf], np.nan,inplace=True)
        resultsData.fillna(0.99,inplace=True)
        resultsData.sort_values(["importance"],inplace=True,ascending=True)
        return resultsData.head(n=top_n)
    else:
        return pd.DataFrame({'model_id':model_id,'train_test_id':train_test_id,'feature':'n/a','importance':0.00}, index=[0])



