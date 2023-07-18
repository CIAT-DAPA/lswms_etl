import os
import sys
import geopandas as gpd
from datetime import datetime
from mongoengine import connect
from ormWP import Adm1, Adm2, Adm3

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

def etl_adm3(dataframe, cols, cols_kebele):
    count = 0
    try:
        print('The process of importing kebeles has begun')
        df_kebele = dataframe[cols].drop_duplicates()
        df_kebele.columns = cols_kebele
        print('Dimension', df_kebele.shape)
        for index, row in df_kebele.iterrows():
            ext_id = str(row['ext_id'])
            if not Adm3.objects(ext_id=ext_id):
                traced_list = [{"created": datetime.now(), "updated": datetime.now(), "enabled": True}]
                print('importing', row['name'], ext_id)
                adm2 = Adm2.objects.get(ext_id=str(row['adm3']))
                if row['name'] is not None:
                    adm3 = Adm3(name=str(row['name']), ext_id=ext_id, adm2=adm2, traced=traced_list, aclimate_id=str(row['aclimate_id']))
                    adm3.save()
                    count += 1
            else:
                print('not imported', row['name'], ext_id)
        print(f'imported {count} kebeles to the database')
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
    # define the columns of the shapefile to be imported, code, and name for the woreda and zone code
    cols_kebele = ['ext_id', 'name', 'adm3', 'aclimate_id']
    cols = ['id_adm4', 'name_adm4', 'id_adm3', 'ws_id']
    etl_adm3(dataframe, cols, cols_kebele)
else:
    log_error('Please check your shapefile and path')
