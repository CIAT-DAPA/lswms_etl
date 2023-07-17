import os, sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from parameters.get_connection import *
from parameters.get_file import *
from imports.import_dataframe import *
import os
import sys
from mongoengine import connect
import geopandas as gpd
from datetime import datetime
from ormWP import Watershed
from ormWP import Adm3

try:
    connect(host=get_mongo_conn_str())
    print('connection established')
except:
    print('no connection')
    print(host=get_mongo_conn_str())
    sys.exit()

def etl_watershed(dataframe,cols,cols_watershed):
    count=0
    print('The process of importing watersheds has begun')
    from datetime import datetime
    df_watershed=dataframe[cols].drop_duplicates()
    df_watershed.columns = cols_watershed
    
    print('Dimension',df_watershed.shape)
    print(df_watershed)
    for index, row in df_watershed.iterrows():
        name = str(row['name'])
        if not Watershed.objects(name=name):
            print('importing', row['adm3'])
            traced_list = [
                {"created": datetime.now(), "updated": datetime.now(), "enabled": True}
            ]
            adm3=Adm3.objects.get(name=str(row['adm3']))
            watershed =Watershed(
                name=row['name'],
                area=row['area'],
                traced=traced_list,
                adm3=adm3
            )
            watershed.save()
            count+=1
        else:
            print('not imported', row['name'])
    print(f'imported {count} watersheds to the database')
    return
    
if get_complete_dataframe_to_import() is not None:
    #define the columns of shape file to be imported code and name for the woreda and zone code
    cols_watershed=['adm3','name','area']
    cols=['name_adm4','wsh_name','wshed_km2']
    
    
    dataframe=get_complete_dataframe_to_import()
    print(etl_watershed(dataframe,cols,cols_watershed))
else:
    print('please check your shape file and path')
    