import os
import sys
import geopandas as gpd
from datetime import datetime
from mongoengine import connect
from ormWP import Watershed
from ormWP import Adm3

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

def etl_watershed(dataframe, cols, cols_watershed):
    count = 0
    try:
        print('The process of importing watersheds has begun')
        df_watershed = dataframe[cols].drop_duplicates()
        df_watershed.columns = cols_watershed
        print('Dimension', df_watershed.shape)
        print(df_watershed)
        for index, row in df_watershed.iterrows():
            name = str(row['name'])
            if not Watershed.objects(name=name):
                print('importing', row['adm3'])
                traced_list = [{"created": datetime.now(), "updated": datetime.now(), "enabled": True}]
                adm3 = Adm3.objects.get(name=str(row['adm3']))
                watershed = Watershed(name=row['name'], area=row['area'], traced=traced_list, adm3=adm3)
                watershed.save()
                count += 1
            else:
                print('not imported', row['name'])
        print(f'imported {count} watersheds to the database')
    except Exception as e:
        log_error(str(e))

try:
    connect(host=get_mongo_conn_str())
    print('Connection established')
except Exception as e:
    log_error(f'Error while establishing the connection with MongoDB: {str(e)}')
    sys.exit()

dataframe = get_complete_dataframe_to_import()
if dataframe is not None:
    # define the columns of the shapefile to be imported, code and name for the woreda and zone code
    cols_watershed = ['adm3', 'name', 'area']
    cols = ['name_adm4', 'wsh_name', 'wshed_km2']
    etl_watershed(dataframe, cols, cols_watershed)
else:
    log_error('Please check your shapefile and path')
