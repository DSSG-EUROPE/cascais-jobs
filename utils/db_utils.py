import sqlalchemy
import pandas as pd
import dbcreds

#Connects to database using credentials file configurations
def connect_to_db():
    return sqlalchemy.create_engine('postgresql://'+dbcreds.user+':'+dbcreds.password+'@'+dbcreds.host+':'+dbcreds.port+'/'+dbcreds.database)

#Read DataFramde from sql table
def read_table(conn,db_schema,table_name):
    return pd.read_sql_table(table_name=table_name,con=conn,schema=db_schema)

#Inserts DataFrame to sql table
def write_table(df, conn, schema, table, if_exists='append', index=False):
    df.to_sql(table, conn, schema=schema, if_exists=if_exists, index=index)
