import pandas as pd
import db_utils

#Cleans application data
    #Removes applications of people whose application date is after max_start_date
    #Removes applications of people who are already employed/part-time employed
#Sample Usage: clean_apps=clean_applications(apps_df,'2016-04-31')
def clean_applications(applications,min_start_date,max_start_date):
    clean_apps = applications.copy()
    clean_apps['app_start_date'] = pd.to_datetime(clean_apps['candidatura_data'])
    clean_apps = clean_apps[(clean_apps['app_start_date'] >= min_start_date)&(clean_apps['app_start_date'] <= max_start_date)]
    clean_apps = clean_apps[~clean_apps['dcategoria'].isin(['EMPREGADO', 'EMPREGADO A TEMPO PARCIAL'])] 
    clean_apps = clean_apps.sort_values(['ute_id','app_start_date'])
    return clean_apps


#Cleans and Filters movements data
    #Removes movements that don't start with an application
    #If apps_series parameter is present, filters the movements which do not belong to the applications in the series
#Sample Usage: clean_movs=clean_movements(movements)
#              clean_movs=clean_movements(movements,apps['table_index']) 
def clean_movements(movements, min_date,max_date,selected_apps_ids=None):
    clean_movs = movements.copy()
    clean_movs = clean_movs[clean_movs['application_id'] != -1] #Removing movements that don't start with an application
    clean_movs['movement_event_date'] = pd.to_datetime(clean_movs['movement_event_date'])
    clean_movs = clean_movs[(clean_movs['movement_event_date'] >= min_date)&(clean_movs['movement_event_date'] <= max_date)]
    if not selected_apps_ids is None: 
        clean_movs = clean_movs[clean_movs['application_id'].isin(selected_apps_ids)]
    clean_movs = clean_movs.sort_values(['ute_id','movement_event_date'])
    return clean_movs

def get_clean_data(conn,db_schema='iefp'):
    movements = db_utils.read_table(conn,db_schema,'movement')
    applications = db_utils.read_table(conn,db_schema,'application')

    apps_min_date = '1980-01-01'
    apps_max_date = '2016-12-30'
    movs_min_date = '1980-01-01'
    movs_max_date = '2017-06-01'
    
    clean_apps = clean_applications(applications,apps_min_date,apps_max_date)
    clean_movs = clean_movements(movements, movs_min_date, movs_max_date, clean_apps['table_index'])

    return (clean_apps,clean_movs)

