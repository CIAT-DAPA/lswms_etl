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
from ormWP import Adm1

try:
    connect(host=get_mongo_conn_str())
    print('connection established')
except:
    print('no connection')
    print(host=get_mongo_conn_str())
    sys.exit()

def etl_adm1(dataframe,cols,cols_zone):
    count=0
    print('The process of importing zones has begun')
    from datetime import datetime
    df_zone=dataframe[cols].drop_duplicates()
    df_zone.columns = cols_zone
    print('Dimension',df_zone.shape)
    for index, row in df_zone.iterrows():
        ext_id = str(row['ext_id'])
        if not Adm1.objects(ext_id=ext_id):
            print('importing', row['name'], ext_id)
            traced_list = [
                {"created": datetime.now(), "updated": datetime.now(), "enabled": True}
            ]
            adm1 = Adm1(
                name=row['name'],
                ext_id=ext_id,
                traced=traced_list
            )
            adm1.save()
            count+=1
        else:
            print('not imported', row['name'], ext_id)
    print(f'imported {count} zones to the database')
    return
    
if get_complete_dataframe_to_import() is not None:
    #define the columns of shape file to be imported code and name for the woreda and zone code
    cols_zone=['ext_id','name']
    cols=['id_adm2','name_adm2']
    
    
    dataframe=get_complete_dataframe_to_import()
    etl_adm1(dataframe,cols,cols_zone)
else:
    print('please check your shape file and path')