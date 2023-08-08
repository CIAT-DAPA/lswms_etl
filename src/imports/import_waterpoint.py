# this etl scrips imports waterpoints to the mongodb by extracting the data from postgresql database
#--------------------------------------------------------------------------------------------------
#import packages and db models 
import os
import sys
import geopandas as gpd
from datetime import datetime
from mongoengine import connect
from ormWP import Watershed
from ormWP.models.waterpoint import Waterpoint

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from parameters.get_connection import *
from parameters.get_file import *
from imports.import_dataframe import *

#handle and log unexpected errors during the importing process
def log_error(message):
    # Create an "error" folder if it doesn't exist
    error_folder = os.path.join(os.path.dirname(__file__), '..', 'error')
    os.makedirs(error_folder, exist_ok=True)

    # Log the error message to a file
    error_log_file = os.path.join(error_folder, 'error_log.txt')
    with open(error_log_file, 'a') as f:
        f.write(f"{datetime.now()}: {message}\n")
#main etl function to import the waterpoints with three arguments
#dataframe 
#cols
#cols_waterpoint
def etl_waterpoint(dataframe, cols, cols_waterpoint):
    count = 0
    try:
        print('---------Importing waterpoints----------------')
        print('----------------------------------------------')
        df_waterpoint = dataframe[cols].drop_duplicates()
        df_waterpoint.columns = cols_waterpoint
        print('Dimension', df_waterpoint.shape)
        print(df_waterpoint)
        for index, row in df_waterpoint.iterrows():
            name = str(row['wsh_name'])
            if not Waterpoint.objects(name=name):
                print('Importing', row['wsh_name'])
                trace = {"created": datetime.now(), "updated": datetime.now(), "enabled": True}

                watershed = Watershed.objects.get(name=str(row['wsh_name']))
                waterpoint = Waterpoint(
                    ext_id=str(row['ext_id']),
                    lat=row['lat'],
                    lon=row['lon'],
                    name=row['wsh_name'],
                    area=row['area'],
                    climatology=[''],
                    other_attributes=[''],
                    trace=trace,
                    watershed=watershed,
                    aclimate_id=''
                )
                waterpoint.save()
                count += 1
            else:
                print('Not imported', row['name'])
        print(f'Imported {count} waterpoints to the database')
    except Exception as e:
        log_error(str(e))

try:
    connect(host=get_mongo_conn_str())
    print('Connection established')
except Exception as e:
    log_error(f'Error while establishing the connection with MongoDB: {str(e)}')
    sys.exit()

dataframe = get_complete_dataframe_to_import()
print('-------------------sample dataframe to be imported---------------')
print(dataframe)
if dataframe is not None:
    # Define the columns of the shapefile to be imported
    cols_waterpoint = ['ext_id', 'lat', 'lon', 'name', 'area', 'wsh_name']
    cols = ['uid', 'y_coord', 'x_coord', 'name', 'wp_km2', 'wsh_name']
    etl_waterpoint(dataframe, cols, cols_waterpoint)
else:
    log_error('Please check your shapefile and path')

