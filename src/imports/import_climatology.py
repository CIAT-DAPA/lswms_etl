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

def log_error(message):
    # Create an "error" folder if it doesn't exist
    error_folder = os.path.join(os.path.dirname(__file__), '..', 'error')
    os.makedirs(error_folder, exist_ok=True)

    # Log the error message to a file
    error_log_file = os.path.join(error_folder, 'error_log.txt')
    with open(error_log_file, 'a') as f:
        f.write(f"{datetime.now()}: {message}\n")

def get_wpmonitored():
    conn = None
    try:
        conn = psycopg2.connect(get_postres_conn_str())
        cur = conn.cursor()
        print('PostgreSQL connection established')
    except Exception as ex:
        log_error('Error in connecting PostgreSQL database: ' + str(ex))
        sys.exit()

    sql = "select location_id,date,day,rain,evap,depth,scaled_depth from location_data where location_id=ANY (select uid from locations where country='Ethiopia');"
    cur.execute(sql)
    results = cur.fetchall()

    # Create a DataFrame with the results
    df = pd.DataFrame(results, columns=['location_id', 'date', 'day', 'rain', 'evap', 'depth', 'scaled_depth'])
    return df

try:
    connect(host=get_mongo_conn_str())
    print('MongoDB connection established')
except Exception as e:
    log_error('Error in connecting MongoDB database: ' + str(e))
    sys.exit()

df = get_wpmonitored()

def get_month(x):
    x = str(x)
    if len(x) == 3:
        month = x[0]
        day = x[1:3]
    elif len(x) == 4:
        month = x[:2]
        day = x[2:4]
    return month

def get_day(x):
    x = str(x)
    if len(x) == 3:
        month = x[0]
        day = x[1:3]
    elif len(x) == 4:
        month = x[:2]
        day = x[2:4]
    return day

def medain_depth(df):
    df_filterd = df[['location_id', 'day', 'rain', 'evap', 'depth', 'scaled_depth']]
    df_grouped = df_filterd.groupby(['location_id', 'day']).median().reset_index()
    df_grouped['month'] = df_grouped.day.apply(get_month)
    df_grouped['day'] = df_grouped.day.apply(get_day)
    return df_grouped

def etl_wpcontent(dataframe):
    count = 0
    print('Running the update function')
    from datetime import datetime
    wp_lists = [int(ext_id.ext_id) for ext_id in Waterpoint.objects]
    dataframe = dataframe[dataframe.location_id.isin(wp_lists)]
    for i in dataframe.location_id.unique():
        climate = []
        for index, row in dataframe[dataframe['location_id'] == i].iterrows():
            trace = {"created": datetime.now(), "updated": datetime.now(), "enabled": True}
            values_list = [{'type': 'depth', 'value': row['depth']}, {'type': 'evp', 'value': row['evap']},
                           {'type': 'rain', 'value': row['rain']}]
            climate_list = [{"month": row['month'], "day": row['day'], "values": values_list}]
            climate.append(climate_list)
        try:
            wp = Waterpoint.objects.get(ext_id=str(row['location_id']))
            print('Updating climatology subset', wp.name)
            wp.update(climatology=climate, trace=trace)
            count += 1
        except Exception as e:
            log_error(f'Error while updating climatology for waterpoint {row["location_id"]}: {str(e)}')

    print(f'Updated {count} waterpoints with climatology data in the database')

etl_wpcontent(medain_depth(df))
