#this etl script imports the historic data from postgress to monitored collection
#import the packages
import psycopg2
from mongoengine import connect
import os,sys
import configparser
import pandas as pd
from ormWP.models.waterpoint import Waterpoint
from ormWP.models.monitored import Monitored
from mongoengine import connect
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from parameters.get_connection import *
from parameters.get_file import *

#connection to the mongo db
try:
    connect(host=get_mongo_conn_str())
    print('connection established')
except:
    print('no connection')
    print(host=get_mongo_conn_str())
    sys.exit()

#we use this list to filter record which are in waterpoint collection
wp_indb=[int(ext_id.ext_id) for ext_id in Waterpoint.objects]

#connection to the postgres
#retrive location data and return the dataframe for both monitoring and climatology
#the function has the argument wp_indb to filter the table from postgres table location_data and it returns a dataframe to be imported to monogo
def get_wpmonitored(wp_indb):
    conn = None
    try:
        conn = psycopg2.connect(get_postres_conn_str())
        cur = conn.cursor()

    except Exception as ex:
        print('error in connection:')
        print(str(ex))


    sql=f"select location_id,date,day,rain,evap,depth,scaled_depth from location_data where location_id=ANY (select uid from locations where country='Ethiopia' and  uid in {tuple(wp_indb)});"
    cur.execute(sql)
    results = cur.fetchall()

    # filter the requreid columns and return the pandas dataframe
    df = pd.DataFrame(results, columns=['location_id', 'date', 'day', 'rain', 'evap', 'depth', 'scaled_depth'])
    return df



#this is the main function for the import process and comments are included for each methods used in the script building
def etl_monitored(df):
    count=0
    print('importing monitored waterpoint to the database')
    from datetime import datetime
    for index, row in df.iterrows():
        #get the date value for each row
        d=row['date']
        #get the collection object filted with the date value
        monitor_object=Monitored.objects(date=datetime(d.year, d.month, d.day))
        #if the date object returns nothing, import the row
        if len(monitor_object)==0:
            waterpoint=Waterpoint.objects.get(ext_id=str(row['location_id']))
            print('importing', waterpoint.name, d ,'monitored data')
            values_list= [{'type': 'depth', 'value':row['depth']}, {'type': 'evp', 'value': row['evap']},{'type': 'rain', 'value': row['rain']},{'type': 'scaled_depth', 'value': row['scaled_depth']}
            ]
            monitored = Monitored(
                date=row['date'],
                values=values_list,
                waterpoint=waterpoint
            )
            monitored.save()
            count+=1
        else:
            #this list used to avoid repeated data import.
            #it checks if the ext_id and date are already in the recored 
            monitor_list=[i.waterpoint.ext_id for i in monitor_object]
            #if the location id for the specifed date row['date'] is not in the the monitor_list excute the import process
            if (str(row['location_id']) not in monitor_list):
                waterpoint=Waterpoint.objects.get(ext_id=str(row['location_id']))
                print('importing', waterpoint.name,'recored date ',d,' monitored data')
                values_list= [{'type': 'depth', 'value':row['depth']}, {'type': 'evp', 'value': row['evap']},{'type': 'rain', 'value': row['rain']},{'type': 'scaled_depth', 'value': row['scaled_depth']}
                ]
                monitored = Monitored(
                date=d,
                values=values_list,
                waterpoint=waterpoint
                )
                monitored.save()
                count+=1

            else:
                print(f"{waterpoint.name},'recored date '{d} not imported already in the database")


    return
#run the get_wpmonitored function to return the dataframe

df=get_wpmonitored(wp_indb)
print(df)
#df=df.drop_duplicates(inplace=True)
#call the etl_monitored to import the dataframe to Monitored collection
etl_monitored(df)
