import pandas as pd
import numpy as np
import sys
from pandas import Series, DataFrame
from datetime import datetime, timedelta
from calendar import monthrange
import db_utils
import iefp_data_utils
from sklearn import preprocessing


#Demographic feature functions
def apps_cancelled_within_n_months(x,n=12):
    return x[(x['movement_type'].isin(['cancellation'])) & (x['months_after_app'] < n)]['application_id'].unique()

def apps_placed_within_n_months(x,n=12):
    return x[(x['movement_result'].isin(['ADMITIDO / COLOCADO'])) & (x['months_after_app'] < n)]['application_id'].unique()

#Add column to show how many months after the application the movement occurred
def monthdelta(d1, d2):
    delta = 0
    while True:
        mdays = monthrange(d1.year, d1.month)[1]
        d1 += timedelta(days=mdays)
        if d1 <= d2:
            delta += 1
        else:
            break
    return delta

def difftime_in_months(timeA,timeB):
    return (timeA-timeB)/np.timedelta64(1, 'M')

def get_ltu_label(apps,movs):
    app_date_dict = dict(zip(apps["table_index"],apps["app_start_date"]))
    movs["app_start_date"] = movs["application_id"].map(app_date_dict)
    movs["months_after_app"] = difftime_in_months(movs["movement_event_date"],movs["app_start_date"])
    
    print "Generating LTU/Non-LTU labels"
    cancelled_12mo = apps_cancelled_within_n_months(movs)
    placed_12mo = apps_placed_within_n_months(movs)
    
    non_ltu_apps = np.unique(np.append(cancelled_12mo,placed_12mo))
    apps['ltu'] = np.logical_not(apps['table_index'].isin(non_ltu_apps))
    last_data_date = max(apps['app_start_date'])
    apps.loc[difftime_in_months(last_data_date,apps['app_start_date']) < 12,'ltu'] = False
    
    return apps

def findSeason(x):
    month=x.month
    if month in [12,1,2]:
        return "Winter"
    if month in [3,4,5]:
        return "Spring"
    if month in [6,7,8]:
        return "Summer"
    if month in [9,10,11]:
        return "Autumn"

def isPortuguese(x):
    if x=="PT":
        return "PT"
    else:
        return "OT"

def lookingForFirstJob(x):
    if "NOVO" in x:
        return "first_job"
    else:
        return "not_first_job"

def createEducationBuckets(x):
    HS=["12"]
    #SL means  able to write, but no school degree,NS means not able to write
    NR=["NS"]
    U6=["SL","04","06"]
    U11=["09","11"]
    
    #The rest is different kinds of higher degree
    if x in HS:
        return "HS"
    if x in NR:
        return "NR"
    if x in U6:
        return "U6"
    if x in U11:
        return "U11"
    else:
        return "MHS"

def civilStatus(x):
    if x=="S":
        return "S"
    if x=="C":
        return "M"
    else :
        return "O"

def ageBucket(x):
    if x<30:
        return "age<30"
    elif x<50:
        return "30<age<50"
    else:
        return "age>50"

def findMainBucket(profession):
    profDict={0:"Miliatary",1:"Managment",2:"Higer level professionals",3:"Lower level professionals",4:"Office jobs",5:"Sales",
              6:"Fising and farming",7:"Craft and trade",8:"Factory work",9:"Elementary"}
    if pd.isnull(profession):
        return "Unknown"
    else:
        professionInt=int(str(profession)[0:1])
        return profDict[professionInt]
    
def findMainEducation(education):
    eduDict={0:"general",1:"Education",2:"ArtsAndHumanities",3:"SocialScienceTradeAndLaw",4:"STEM",5:"EngineeringAndConstruction",6:"Agriculture",7:"HealthAndSocialProtection",8:"Service",9:"Unkown"}
    if pd.isnull(education):
        return eduDict[9]
    else:
        professionInt=int(str(education)[0:1])
        return eduDict[professionInt]

def preferredJobRegion(letter):
    if letter == "C":
        return "NearJobCenter"
    elif letter == "R":
        return "GreaterLisbonArea"
    elif letter == "N":
        return "Available"
    elif pd.isnull(letter):
        return "Unknown"
    else:
        return "Error"

def employmentPlan(letter):
    if letter == "S":
        return "exists"
    elif letter =="N":
        return "no plan"
    else:
        return "Error"
    
def applicationOrigin(origin):
    if origin == "SIGAE":
        return "SIGAE"
    elif origin == "LSE":
        return "LSE"
    elif pd.isnull(origin):
        return "unknown"
    else:
        return "Error"
    
def intendedRegime(regime):
    if regime == "COMPLETO":
        return "Full-time"
    elif regime == "PARCIAL":
        return "Part-time"
    else:
        return "Error"

def dependentsBucket(n):
    if n == 0:
        return "0"
    elif n == 1:
        return "1"
    elif n == 2:
        return "2"
    elif n == 3:
        return "3"
    elif pd.isnull(n):
        return "Unknown"
    else:
        return "4+"
    
def experienceBuckets(n):
    if pd.isnull(n):
        return "Unknown"
    elif n == 0:
        return "0"
    elif n<24:
        return "<2yr"
    elif n<60:
        return "<5yr"
    elif n<120:
        return "<10yr"
    elif n<240:
        return "<20yr"
    else:
        return "20+yr"

#Dynamic variable functions

def generate_cumsum(movs):
    movs_count = movs.drop(['movement_subtype','movement_result','movement_index','ute_id'], axis=1)
    movs_count['movements'] = 1
    movs_count = pd.get_dummies(movs_count, columns=['movement_type'])
    movs_cumsum = movs_count.groupby(['application_id','movement_event_date']).sum().groupby(level=[0]).cumsum()
    movs_cumsum.reset_index(level=[0,1],inplace=True)
    column_names = {'movement_type_application':'apps_so_far', 'movement_type_cancellation':'cancellations_so_far',\
                   'movement_type_category_change':"cat_changes_so_far", 'movement_type_convocation':'convocations_so_far',\
                   'movement_type_intervention':'interventions_so_far','movement_type_interview':'interviews_so_far',}
    movs_cumsum.rename(columns=column_names,inplace=True)
    if 'cancellations_so_far' in movs_cumsum.columns:
        #Remove because test set should not have cancellations 
        movs_cumsum.drop(['cancellations_so_far'],axis=1,inplace=True)
    if 'apps_so_far' in movs_cumsum.columns:
        #Remove because test set should not have cancellations 
        movs_cumsum.drop(['apps_so_far'],axis=1,inplace=True) 
   
    return movs_cumsum

def generate_movs_so_far(extended_data, movs):
    movs_cumsum = generate_cumsum(movs)
    
    extended_trimmed = extended_data.drop(['app_start_date','ltu'], axis=1)
    extended_trimmed['entry_type'] = 'observation'
    movs_cumsum['entry_type'] = 'mov_date'
    
    #combine the extended data (has observation dates) with movements (has cumsum data)
    movs_so_far = pd.concat([extended_trimmed, movs_cumsum])
    movs_so_far['sort_date'] = movs_so_far['ref_date'].fillna(movs_so_far['movement_event_date'])
    
    #sort by date and forward fill (so that observation has cumsums from the closest movement_event_date before it)
    movs_so_far.sort_values(['application_id','sort_date','entry_type'], inplace=True)
    movs_so_far.drop(['movement_event_date','ref_date'], axis=1,inplace=True)
    movs_so_far.fillna(method='ffill', inplace=True)
    
    return movs_so_far[movs_so_far['entry_type']=="observation"]

# HISTORICAL FEATURES FUNCTIONS
def generate_apps_cumsum(apps_df):
    apps_count = apps_df[['table_index','ute_id','app_start_date']]
    apps_count.loc[:,'apps']=1
    apps_cumsum = apps_count.groupby(['ute_id','table_index','app_start_date']).sum().groupby(level=[0]).cumsum()
    apps_cumsum.reset_index(level=[2], inplace = True)
    apps_cumsum.rename(columns = {'apps':'apps_cumsum'}, inplace=True)
    apps_cumsum.reset_index(level=[0,1], inplace = True)
    apps_cumsum['date_type']='app_start_date'
    apps_cumsum['prev_apps_cumsum'] = apps_cumsum['apps_cumsum']-1
    return apps_cumsum

def generate_markers(apps_df, historical_limit):
    limit = pd.Timedelta(historical_limit)
    apps_markers = pd.DataFrame(apps_df.groupby(['ute_id','table_index']).app_start_date.max())
    apps_markers['app_st_date'] = apps_markers['app_start_date']
    apps_markers.set_index(['app_st_date'], append=True,inplace=True)
    apps_markers['historical_st_date'] = apps_markers['app_start_date']-limit
    apps_markers = pd.DataFrame(apps_markers.stack())
    apps_markers.reset_index(level=[0,1,2,3], inplace = True)
    apps_markers.rename(columns = {'level_3':'date_type', 0:'date'}, inplace=True)
    apps_markers.sort_values(['ute_id','date'], inplace=True)
    return apps_markers

def generate_historical(apps_df, historical_limit):
    
    apps_cumsum = generate_apps_cumsum(apps_df)
    apps_markers = generate_markers(apps_df, historical_limit)
    
    apps_historical = pd.merge(apps_markers, apps_cumsum, how='left', left_on= ['ute_id','table_index','date_type','app_st_date'], right_on=['ute_id','table_index','date_type', 'app_start_date']).drop(['apps_cumsum'], axis=1)
    apps_historical['prev_apps_cumsum'].fillna(method='bfill', inplace=True)
    apps_historical = pd.pivot_table(apps_historical, values = "prev_apps_cumsum", index = ['ute_id', 'table_index', 'app_st_date'], columns = 'date_type')
    apps_historical['prev_apps_within_limit'] = apps_historical['app_start_date'] - apps_historical['historical_st_date']
    #apps_historical['prev_apps_within_limit']=preprocessing.scale(apps_historical["prev_apps_within_limit"], copy=False)
    apps_historical = apps_historical.reset_index(level=[0,1]).drop(['historical_st_date','app_start_date'], axis=1)
    return apps_historical


# SYSTEM FEATURES FUNCTIONS
def generate_system_info(apps):
    system_info = apps.copy()
    system_info['app_year']=system_info['app_start_date'].apply(lambda x: x.year)
    system_info['app_month']=system_info['app_start_date'].apply(lambda x: x.month)
    system_info = system_info.groupby(['app_year','app_month']).anomes.count()
    return system_info


#Functions to add features to a matrix base

def add_demographics(feature_matrix, apps):
    #Create DF with the demographic features per application_id
    demo_df = apps[["table_index"]]    
    demo_df["age"] = apps["ute_idade"] #redo age variable to make dyanamic?
    demo_df["gender"] = apps["sexo"]
    demo_df["is_re_registriation"] = apps["candidatura_rinsc"]
    demo_df["soc_ben"] = apps["sub_rsi"]
    demo_df['region'] = apps['dfreguesia']
    #demo_df["age2"]=demo_df["age"]**2
    demo_df["age"]=preprocessing.scale(demo_df["age"].astype(float),copy=False)
    #demo_df["age2"]=preprocessing.scale(demo_df["age2"].astype(float),copy=False)
    demo_df["season"]=apps["app_start_date"].apply(findSeason)
    demo_df["nationality"]=apps["cnacionalidade"].apply(isPortuguese)
    demo_df["education"]=apps["chabilitacao_escolar"].apply(createEducationBuckets) 
    demo_df["first_job"]=apps["dcategoria"].apply(lookingForFirstJob)
    demo_df["time_since_exit"]=(pd.to_datetime(apps["candidatura_data"])-pd.to_datetime(apps["reinscricao_ult_saida_data"])).dt.days
    demo_df["time_since_exit"].fillna(500,inplace=True)
    demo_df["time_since_exit"]=pd.to_numeric(demo_df["time_since_exit"])
    demo_df["time_since_exit"]=preprocessing.scale(demo_df["time_since_exit"], copy=False)
    demo_df["is_disabled"]=apps["cdeficiencia"].apply(lambda x: "N" if x==0 else "S")
    demo_df["civil_status"]=apps["ute_estado_civil"].apply(civilStatus)
    demo_df["has_course_area"]=apps["darea_curso_tabela_em_activo"]
    demo_df["has_course_area"].fillna(0,inplace=True)
    demo_df["has_course_area"]=demo_df["has_course_area"].apply(lambda x: "S" if x==0 else "N")
    demo_df["has_prof_area"]=apps["darea_formacao_tabela_em_activo"]
    demo_df["has_prof_area"].fillna(0,inplace=True)
    demo_df["has_prof_area"]=demo_df["has_prof_area"].apply(lambda x: "S" if x==0 else "N")
    demo_df["age_category"]=demo_df["age"].apply(ageBucket)
    demo_df["number_dependents"]=apps["ute_nr_pessoas_cargo"].astype(float).fillna(0)
    demo_df["number_dependents"]=preprocessing.scale(demo_df["number_dependents"], copy=False)
    demo_df["experience_intended_prof"]=apps["candidatura_prof_pret_tempo_pratica"].astype(float).fillna(0)
    demo_df["experience_intended_prof"]=preprocessing.scale(demo_df["experience_intended_prof"], copy=False)
    demo_df["experience_prev_prof"]=apps["sit_anterior_prof_tempo_pratica"].astype(float).fillna(0)
    demo_df["experience_prev_prof"]=preprocessing.scale(demo_df["experience_prev_prof"], copy=False) 
    demo_df["intended_prof_1"]=apps["cnp_pretendida"].apply(findMainBucket)
    demo_df["intended_prof_2"]=apps["cpp_pretendida"].apply(findMainBucket)
    demo_df["intended_prof"]=np.where(pd.isnull(demo_df["intended_prof_1"]),demo_df["intended_prof_2"],demo_df["intended_prof_1"])
    del demo_df["intended_prof_1"]
    del demo_df["intended_prof_2"]  
    demo_df["previous_prof_1"]=apps["cnp_anterior"].apply(findMainBucket)
    demo_df["previous_prof_2"]=apps["cpp_anterior"].apply(findMainBucket)
    demo_df["previous_prof"]=np.where(pd.isnull(demo_df["previous_prof_1"]),demo_df["previous_prof_2"],demo_df["previous_prof_1"])
    del demo_df["previous_prof_1"]
    del demo_df["previous_prof_2"]   
    demo_df["training_area"]= apps["carea_formacao_tabela_em_activo"].apply(findMainEducation)   
   #demo_df['app_start_date'] = pd.to_datetime(apps['app_start_date'])  #already in the extended data
    demo_df['preferred_location']=apps['candidatura_local_trabalho'].apply(preferredJobRegion)
    #demo_df['app_year'] = apps['app_start_date'].apply(lambda x: x.year)
    #demo_df['app_month'] = apps['app_start_date'].apply(lambda x: x.month)
    demo_df['employment_plan'] = apps['ute_plano_emprego'].apply(employmentPlan)
    demo_df['prev_employment_plan'] = apps['ute_plano_emprego_anterior'].apply(employmentPlan)
    demo_df['app_origin'] = apps['candidatura_origem'].apply(applicationOrigin)
    demo_df['intended_regime'] = apps['dregime_contrato_pretendido'].apply(intendedRegime)
    demo_df['dependents_bucket'] = apps['ute_nr_pessoas_cargo'].astype(float).apply(dependentsBucket)
    demo_df['exp_intended_prof_buckets'] = apps['candidatura_prof_pret_tempo_pratica'].astype(float).apply(experienceBuckets)
    demo_df['exp_previous_prof_buckets'] = apps['sit_anterior_prof_tempo_pratica'].astype(float).apply(experienceBuckets)
   #map to feature matrix using application ID
    feature_matrix = pd.merge(feature_matrix, demo_df, how='left', left_on='application_id', right_on='table_index')
    feature_matrix.drop('table_index',axis=1,inplace=True) 
    return feature_matrix

def add_basic(feature_matrix, apps):
    #Create DF with the demographic features per application_id
    demo_df = apps[["table_index"]]    
    demo_df["age"] = apps["ute_idade"] #redo age variable to make dyanamic?
    #demo_df["gender"] = apps["sexo"]
    demo_df["is_re_registriation"] = apps["candidatura_rinsc"]
    demo_df["soc_ben"] = apps["sub_rsi"]
    #demo_df["age2"]=demo_df["age"]**2
    demo_df["age"]=preprocessing.scale(demo_df["age"].astype(float),copy=False)
    #demo_df["age2"]=preprocessing.scale(demo_df["age2"].astype(float),copy=False)
    #demo_df["season"]=apps["app_start_date"].apply(findSeason)
    demo_df["nationality"]=apps["cnacionalidade"].apply(isPortuguese)
    demo_df["education"]=apps["chabilitacao_escolar"].apply(createEducationBuckets) 
    #demo_df["first_job"]=apps["dcategoria"].apply(lookingForFirstJob)
    demo_df["time_since_exit"]=(pd.to_datetime(apps["candidatura_data"])-pd.to_datetime(apps["reinscricao_ult_saida_data"])).dt.days
    demo_df["time_since_exit"].fillna(500,inplace=True)
    demo_df["time_since_exit"]=pd.to_numeric(demo_df["time_since_exit"])
    demo_df["time_since_exit"]=preprocessing.scale(demo_df["time_since_exit"], copy=False)
    demo_df["is_disabled"]=apps["cdeficiencia"].apply(lambda x: "N" if x==0 else "S")
    demo_df["number_dependents"]=apps["ute_nr_pessoas_cargo"].astype(float).fillna(0)
    demo_df["number_dependents"]=preprocessing.scale(demo_df["number_dependents"], copy=False)
    
    #map to feature matrix using application ID
    feature_matrix = pd.merge(feature_matrix, demo_df, how='left', left_on='application_id', right_on='table_index')
    feature_matrix.drop('table_index',axis=1,inplace=True) 
    return feature_matrix


def add_system_features(feature_matrix,system_info):
    feature_matrix['total_apps_in_month'] = feature_matrix['ref_date'].apply(lambda x: system_info[x.year, x.month])
    return feature_matrix

def add_historical_features(feature_matrix, historical_info):
    pass

def add_app_length_feature(feature_matrix, extended_data, movs):
    feature_matrix['months_so_far'] = (feature_matrix['ref_date']-feature_matrix['app_start_date']).apply(lambda x: x.days/30)
    return feature_matrix

def add_dynamic_features(feature_matrix, extended_data, movs, include_app_length):
    #generate movs_so_far variables
    movs_so_far = generate_movs_so_far(extended_data, movs)
    feature_matrix = pd.merge(feature_matrix, movs_so_far, how='left', left_on = ['application_id', 'ref_date'],\
                             right_on=['application_id','sort_date'])
    feature_matrix.drop(['sort_date', 'entry_type'], inplace=True, axis=1)
    
    #faster months since app
    if (include_app_length):
        feature_matrix['months_so_far'] = (feature_matrix['ref_date']-feature_matrix['app_start_date']).apply(lambda x: x.days/30)
    return feature_matrix

def add_system_features(feature_matrix,system_info):
    feature_matrix['total_apps_in_month'] = feature_matrix['ref_date'].apply(lambda x: system_info[x.year, x.month])
    feature_matrix['total_apps_in_month'] =preprocessing.scale(feature_matrix['total_apps_in_month'])
    return feature_matrix

def add_historical_features(feature_matrix,historical_info):
    historical_dict = dict(zip(historical_info['table_index'], historical_info['prev_apps_within_limit']))
    feature_matrix['prev_apps_within_limit']=feature_matrix['application_id'].map(historical_dict)
    feature_matrix['prev_apps_within_limit']=preprocessing.scale(feature_matrix["prev_apps_within_limit"], copy=False)
    return feature_matrix

#Generate matrix function that takes apps and movs tables as inputs
def generate_matrix(extended_data, apps_cropped, movs_cropped, feature_set_list,system_info,historical_info):

    print "Generating base matrix" #generate basic matrix including appID, application date, months so far, and label
    feature_matrix = extended_data.copy()
    if "basic" in feature_set_list:
        print "adding basic features"
        feature_matrix = add_basic(feature_matrix, apps_cropped)
    if "demographics" in feature_set_list:
        print "Adding demographic features"
        feature_matrix = add_demographics(feature_matrix, apps_cropped)
            
    if "system" in feature_set_list:
        print "Adding system features"
        feature_matrix = add_system_features(feature_matrix,system_info)

    if "historical" in feature_set_list:
        print "Adding historical features"
        feature_matrix = add_historical_features(feature_matrix,historical_info)    

    if "dynamic" in feature_set_list:
        print "Adding dynamic features"
        feature_matrix = add_dynamic_features(feature_matrix,extended_data,movs_cropped, True)

    if "app-length" in feature_set_list:
        print "Adding app-length feature"
        feature_matrix = add_app_length_feature(feature_matrix, extended_data, movs_cropped)

    if "dynamic-without-app-length" in feature_set_list:
        print "Adding dynamic features without app-length features"
        feature_matrix = add_dynamic_features(feature_matrix, extended_data, movs_cropped, False)

    if "dynamic-with-app-length" in feature_set_list:
        print "Adding dynamics with app-length features"
        feature_matrix = add_dynamic_features(feature_matrix,extended_data,movs_cropped, True)
 
    #Removing Irrelevant features and create dummies for categorical variables
    irrelevant_features = ['app_start_date']
    feature_matrix.drop(irrelevant_features,1,inplace=True)
    
    #Take observation identification variables and set as index
    feature_matrix.set_index(['application_id','ref_date'], inplace=True)
    
    feature_matrix = pd.get_dummies(feature_matrix,drop_first=True)
    return feature_matrix
   
