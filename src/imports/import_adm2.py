import os, sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from parameters.get_connection import *
from parameters.get_file import *
from imports.import_dataframe import *
from mongoengine import connect
import geopandas as gpd
from datetime import datetime
from ormWP import Adm1, Adm2


def log_error(message):
    # Create an "error" folder if it doesn't exist
    error_folder = os.path.join(os.path.dirname(__file__), '..', 'error')
    os.makedirs(error_folder, exist_ok=True)

    # Log the error message to a file
    error_log_file = os.path.join(error_folder, 'error_adm2_log.txt')
    with open(error_log_file, 'a') as f:
        f.write(f"{datetime.now()}: {message}\n")

from datetime import datetime

def etl_adm2(dataframe, cols, cols_woreda):
    count_imported = 0
    count_updated = 0
    try:
        print('The process of importing woredas has begun')
        df_woreda = dataframe[cols].drop_duplicates()
        df_woreda.columns = cols_woreda
        print('Dimension', df_woreda.shape)
        for index, row in df_woreda.iterrows():
            ext_id = str(row['ext_id'])
            name = row['name']
            adm2 = Adm2.objects(ext_id=ext_id).first()

            if not adm2:
                print('Importing', name, ext_id)
                trace = {"created": datetime.now(), "updated": datetime.now(), "enabled": True}
                adm1 = Adm1.objects.get(ext_id=str(row['adm1']))
                adm2 = Adm2(name=name, ext_id=ext_id, adm1=adm1, trace=trace)
                adm2.save()
                count_imported += 1
            elif adm2.name != name: 
                print('Updating', name, ext_id)
                adm2.name = name  # Update the name if it has changed
                adm2.trace.updated = datetime.now()  # Update the "updated" field in the trace
                adm2.save()
                count_updated += 1

        print(f'Imported {count_imported} woredas to the database')
        print(f'Updated {count_updated} woredas in the database')
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
    # Define the columns of the shapefile to be imported, code, and name for the zone and woreda
    cols_woreda = ['ext_id', 'name', 'adm1']
    cols = ['id_adm3', 'name_adm3', 'id_adm2']
    etl_adm2(dataframe, cols, cols_woreda)
else:
    log_error('Please check your shapefile and path')