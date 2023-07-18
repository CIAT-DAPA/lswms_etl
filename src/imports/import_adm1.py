import os, sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from parameters.get_connection import *
from parameters.get_file import *
from imports.import_dataframe import *
from mongoengine import connect
from datetime import datetime
from ormWP import Adm1

def log_error(message):
    # Create an "error" folder if it doesn't exist
    error_folder = os.path.join(os.path.dirname(__file__), '..', 'error')
    os.makedirs(error_folder, exist_ok=True)

    # Log the error message to a file
    error_log_file = os.path.join(error_folder, 'error_log.txt')
    with open(error_log_file, 'a') as f:
        f.write(f"{datetime.now()}: {message}\n")

def etl_adm1(dataframe, cols, cols_zone):
    count = 0
    try:
        print('The process of importing zones has begun')
        df_zone = dataframe[cols].drop_duplicates()
        df_zone.columns = cols_zone
        print('Dimension', df_zone.shape)
        for index, row in df_zone.iterrows():
            ext_id = str(row['ext_id'])
            if not Adm1.objects(ext_id=ext_id):
                print('importing', row['name'], ext_id)
                traced_list = [{"created": datetime.now(), "updated": datetime.now(), "enabled": True}]
                adm1 = Adm1(name=row['name'], ext_id=ext_id, traced=traced_list)
                adm1.save()
                count += 1
            else:
                print('not imported', row['name'], ext_id)
        print(f'imported {count} zones to the database')
    except Exception as e:
        log_error(str(e))

try:
    connect(host=get_mongo_conn_str())
    print('connection established')
except Exception as e:
    log_error(f'Error while establishing MongoDB connection: {str(e)}')
    sys.exit()

dataframe = get_complete_dataframe_to_import()
if dataframe is not None:
    # define the columns of shape file to be imported code and name for the woreda and zone code
    cols_zone = ['ext_id', 'name']
    cols = ['id_adm2', 'name_adm2']
    etl_adm1(dataframe, cols, cols_zone)
else:
    log_error('please check your shape file and path')