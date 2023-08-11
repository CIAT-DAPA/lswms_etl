#this etl script updates the climatology of depth ande evp elements from the monitored  waterpoint 
#-----------------------------------------
import os
import sys
import psycopg2
import configparser
import pandas as pd
from ormWP.models.waterpoint import Waterpoint
from mongoengine import connect
from datetime import datetime
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from parameters.get_connection import *
from parameters.get_file import *
from imports.import_dataframe import *

#log_error which logs the errors in this scrip occured during the updating the median
def log_error(message):
    # Create an "error" folder if it doesn't exist
    error_folder = os.path.join(os.path.dirname(__file__), '..', 'error')
    os.makedirs(error_folder, exist_ok=True)

    # Log the error message to a file
    error_log_file = os.path.join(error_folder, 'error_log.txt')
    with open(error_log_file, 'a') as f:
        f.write(f"{datetime.now()}: {message}\n")

#connection function to mongodb
try:
    connect(host=get_mongo_conn_str())
    print('MongoDB connection established')
except Exception as e:
    log_error('Error in connecting MongoDB database: ' + str(e))
    sys.exit()


#we use this list to filter record which are in waterpoint collection
wp_indb=[int(ext_id.ext_id) for ext_id in Waterpoint.objects]

#this function connects to postgress to retrive the  location data(monitored data) and returns as a pandas dataframe
def get_wpmonitored(wp_indb):
    conn = None
    try:
        conn = psycopg2.connect(get_postres_conn_str())
        cur = conn.cursor()
        print('PostgreSQL connection established')
    except Exception as ex:
        log_error('Error in connecting PostgreSQL database: ' + str(ex))
        sys.exit()

    sql = f"select location_id,date,day,rain,evap,depth,scaled_depth from location_data where location_id=ANY (select uid from locations where country='Ethiopia' and  uid in {tuple(wp_indb)});"
    cur.execute(sql)
    results = cur.fetchall()

    # Create a DataFrame with the results
    df = pd.DataFrame(results, columns=['location_id', 'date', 'month_day', 'rain', 'evap', 'depth', 'scaled_depth'])
    return df


#call and get the dataframe as input for median process
df = get_wpmonitored(wp_indb)

#this function process takes dataframe parameter as input, assign new day and month inputs from the dataframe, and calculates the median and returns as a dataframe
def medain_depth(df):
    df['date']=pd.to_datetime(df['date'])
    df=df.assign(month=df.date.dt.month,day=df.date.dt.day)
    df_filterd=df[['location_id','month_day','month','day','rain', 'evap','depth', 'scaled_depth']]
    df_grouped=df_filterd.groupby(['location_id','month_day']).median().reset_index()
    df_grouped=df_grouped.drop(columns=['month_day'])
    return df_grouped

#this function process the input to mongodb Waterpoint collection and climatology list field
def etl_wpcontent(dataframe):
    count = 0
    print('Running the update function')
    from datetime import datetime
    for i in dataframe.location_id.unique():
        climate = []
        for index, row in dataframe[dataframe['location_id'] == i].iterrows():
            trace = {"created": datetime.now(), "updated": datetime.now(), "enabled": True}
            values_list = [{'type': 'depth', 'value': row['depth']}, {'type': 'evp', 'value': row['evap']},
                           {'type': 'rain', 'value': row['rain']},{'type': 'scaled_depth', 'value': row['scaled_depth']}]
            climate_list = [{"month": row['month'], "day": row['day'], "values": values_list}]
            climate.append(climate_list)
        try:
            wp = Waterpoint.objects.get(ext_id=str(int(row['location_id'])))
            print('Updating climatology subset', wp.name)
            wp.update(climatology=climate, trace=trace)
            count += 1
        except Exception as e:
            log_error(f'Error while updating climatology for waterpoint {row["location_id"]}: {str(e)}')

    print(f'Updated {count} waterpoints with climatology data in the database')

#call the function to make the inputs to the mongodb Waterpoint collection
etl_wpcontent(medain_depth(df))
