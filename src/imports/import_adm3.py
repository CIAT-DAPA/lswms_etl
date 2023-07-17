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
from ormWP import Adm2
from ormWP import Adm3

try:
    connect(host=get_mongo_conn_str())
    print('connection established')
except:
    print('no connection')
    print(host=get_mongo_conn_str())
    sys.exit()

def etl_adm3(dataframe,cols,cols_kebele):
    count=0
    print('The process of importing woredas has begun')
    from datetime import datetime
    df_kebele=dataframe[cols].drop_duplicates()
    df_kebele.columns = cols_kebele
    print('Dimension',df_kebele.shape)
    for index, row in df_kebele.iterrows():
         if not Adm3.objects(ext_id=str(row['ext_id'])):
            traced_list = [
                {"created": datetime.now(), "updated": datetime.now(), "enabled": True}
            ]
            print('importing',row['name'],row['ext_id'])
            adm2=Adm2.objects.get(ext_id=str(row['adm3']))
            if row['name'] is not None:
                adm3=Adm3(name=str(row['name']),ext_id=str(row['ext_id']),adm2=adm2,traced=traced_list,aclimate_id=str(row['aclimate_id']))
                adm3.save()
                count+=1
         else:
            print('not imported',row['name'],row['ext_id'])
    print(f'imported {count} kebeles to the database')
    return
    
if get_complete_dataframe_to_import() is not None:
    #define the columns of shape file to be imported code and name for the woreda and zone code
    cols_kebele=['ext_id','name','adm3','aclimate_id']
    cols=['id_adm4','name_adm4','id_adm3','ws_id']
    
    
    dataframe=get_complete_dataframe_to_import()
    etl_adm3(dataframe,cols,cols_kebele)
else:
    print('please check your shape file and path')