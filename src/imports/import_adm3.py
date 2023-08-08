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
    count_imported = 0
    count_updated = 0
    try:
        print('The process of importing kebeles has begun')
        df_kebele = dataframe[cols].drop_duplicates()
        df_kebele.columns = cols_kebele
        print('Dimension', df_kebele.shape)
        for index, row in df_kebele.iterrows():
            ext_id = str(row['ext_id'])
            name = str(row['name'])
            adm3 = Adm3.objects(ext_id=ext_id).first()

            if not adm3:
                trace = {"created": datetime.now(), "updated": datetime.now(), "enabled": True}
                print('Importing', name, ext_id)
                adm2 = Adm2.objects.get(ext_id=str(row['adm3']))
                adm3 = Adm3(name=name, ext_id=ext_id, adm2=adm2, trace=trace,)
                adm3.save()
                count_imported += 1
            elif adm3.name != name:
                print('Updating', name, ext_id)
                adm3.name = name  # Update the name if it has changed
                adm3.trace.updated = datetime.now()  # Update the "updated" field in the traced list
                adm3.save()
                count_updated += 1

        print(f'Imported {count_imported} kebeles to the database')
        print(f'Updated {count_updated} kebeles in the database')
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
        #define the columns of shape file to be imported code and name for the woreda and zone code
    cols_kebele=['ext_id','name','adm3']
    df,columns= get_dataframe_shp()
    del df
    cols= columns[-1]
    etl_adm3(dataframe, cols, cols_kebele)
else:
    log_error('Please check your shapefile and path')
