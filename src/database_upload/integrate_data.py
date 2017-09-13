import os
import sys
module_path = os.path.abspath(os.path.join('../../utils/'))
if module_path not in sys.path:
    sys.path.append(module_path)

import pandas as pd
import numpy as np
import psycopg2 as pg
import dbcreds
from sqlalchemy import create_engine
import io
import sys

#Static Variables
MIN_NUM_ARGS = 2

#Functions

def printUsage():
    print "python integrate_data.py <schema-name>"

#Generates a data frame from a database selection
def generate_df(schema, table_list, conn):
    df_dict = {}
    for table in table_list:
        df_dict[table] = pd.read_sql('select * from '+schema+'.'+table, con=conn)
    return df_dict

#Adds a column for current table index
def generate_table_index(table):
    table_new = table.copy()
    table_new['table_index']=table_new.index
    return table_new

#Adds movement data to movement data frame
def add_event_data(iefp_data_frame,event_data_frame,movement_type,cols_list):
    print "Adding " + movement_type + " data to movements table"
    curr_event_df = pd.DataFrame()
    index=0
    for col in cols_list:
        if (col == ""):
            curr_event_df["Unnamed" + str(index)] = ""
        else:
            curr_event_df[col]=event_data_frame[[col]].copy()
        index+=1
    curr_event_df["movement_type"]=movement_type
    curr_event_df.columns=["ute_id","movement_event_date","movement_index","movement_subtype","movement_result","movement_type"]
    return iefp_data_frame.append(curr_event_df, ignore_index=True)

def set_application_ids_to_movements(movements_df):
    application_id=0
    previous_id=-1

    events_applications=[0]*movements_df.shape[0]

    counter=0
    for index, row in movements_df.iterrows():
        id=int(row["ute_id"])
        if row["movement_type"]=="application":
            application_id=row['movement_index']
        elif id!=previous_id:
            application_id=-1
        previous_id=row["ute_id"]
        events_applications[counter]=application_id
        counter+=1

    movements_df["application_id"]=pd.Series(events_applications).values
    return movements_df

def parse_out_of_range_date(x):
    if x == '':
        return ''
    else:
        try:
            return pd.to_datetime(x).strftime('%Y-%m-%d')
        except:
            return pd.to_datetime('1900-01-01').strftime('%Y-%m-%d')

#Check Script Arguments
if len(sys.argv) < MIN_NUM_ARGS:
    print "Wrong Usage!"
    printUsage()
    exit(1)

#Read Variables
schema_name = sys.argv[1]
MOVEMENTS_TABLE_NAME = schema_name+".movement"
CLEAR_EVENTS_TABLE_STMT = "DELETE FROM " + MOVEMENTS_TABLE_NAME + ";"

# connect to DB
conn = pg.connect(dbname = dbcreds.database, host=dbcreds.host, user=dbcreds.user, password = dbcreds.password)

# read all tables from DB and return as dataframes
print "Reading tables from DB..." 
df_dict = generate_df(schema_name,['application','cancellation','category_change','convocation','intervention','interview'], conn)

# name dataframes
apps_df = df_dict['application']
cancel_df = df_dict['cancellation']
cat_changes_df = df_dict['category_change']
convocation_df = df_dict['convocation']
interventions_df = df_dict['intervention']
interviews_df = df_dict['interview']


event_data_frames=[apps_df,cancel_df,cat_changes_df,convocation_df,interventions_df,interviews_df]

#adding indexes to the datafiles
cancel_df.rename(columns={"id":"ute_id"},inplace=True)

#adding the seperate events to one datfile
iefp_data_frame=pd.DataFrame()
iefp_data_frame= add_event_data(iefp_data_frame,apps_df,"application",["ute_id","candidatura_data","table_index","dcategoria",""])
iefp_data_frame= add_event_data(iefp_data_frame,cancel_df,"cancellation",["ute_id","anulacao_data","table_index","dmotivo_anulacao",""])
iefp_data_frame= add_event_data(iefp_data_frame,cat_changes_df,"category_change",["ute_id","mo_data_movimento","table_index","dcategoria_anterior","dcategoria"])
iefp_data_frame= add_event_data(iefp_data_frame,convocation_df,"convocation",["ute_id","convocatoria_em","table_index","dtipo_convocatoria","dresultado_convocatoria"])
iefp_data_frame= add_event_data(iefp_data_frame,interviews_df,"interview",["ute_id","apresentacao_data","table_index","oferta_nr","dresultado_apresentacao"])
iefp_data_frame= add_event_data(iefp_data_frame,interventions_df,"intervention",["ute_id","intervencao_data","table_index","intervencao_codigo_d","dresultado_intervencao"])

#Fixing/Parsing movement dates
#iefp_data_frame=iefp_data_frame[:100000]
print "Fixing dates"
iefp_data_frame['movement_event_date'] = iefp_data_frame.movement_event_date.apply(parse_out_of_range_date)

#Sorting Data
print "Sorting data"
iefp_data_frame=iefp_data_frame.sort_values(["ute_id","movement_event_date","movement_type", "movement_index"])

#Assigning application ids to events
#iefp_data_frame=iefp_data_frame[:100] #if testing, uncoment this line to try with fewer rows first
print "Assigning application ids to events"
iefp_data_frame=set_application_ids_to_movements(iefp_data_frame)
iefp_data_frame=iefp_data_frame.reindex(columns=["ute_id","movement_event_date","application_id","movement_type","movement_subtype","movement_result","movement_index"])


#Inserting data into dabase

print "Inserting movements table into DB" 
#Writing data to IO Stream
f=io.BytesIO()
iefp_data_frame.to_csv(f,index=False,header=False,sep=';')
f.seek(0)

try:
    db_cursor = conn.cursor()
    db_cursor.execute(CLEAR_EVENTS_TABLE_STMT)
    db_cursor.copy_from(f, MOVEMENTS_TABLE_NAME, sep=';')
    db_cursor.close()
    conn.commit()
except (Exception, pg.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        conn.close()
