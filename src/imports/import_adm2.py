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

try:
    connect(host=get_mongo_conn_str())
    print('connection established')
except:
    print('no connection')
    print(host=get_mongo_conn_str())
    sys.exit()

def etl_adm2(dataframe,cols,cols_woreda):
    count=0
    print('The process of importing woredas has begun')
    from datetime import datetime
    df_woreda=dataframe[cols].drop_duplicates()
    df_woreda.columns = cols_woreda
    print('Dimension',df_woreda.shape)
    for index, row in df_woreda.iterrows():
         if not Adm2.objects(ext_id=str(row['ext_id'])):
            traced_list = [
                {"created": datetime.now(), "updated": datetime.now(), "enabled": True}
            ]
            print('importing',row['name'],row['ext_id'])
            adm1=Adm1.objects.get(ext_id=str(row['adm1']))
            adm2=Adm2(name=row['name'],ext_id=str(row['ext_id']),adm1=adm1,traced=traced_list)
            adm2.save()
            count+=1
         else:
            print('not imported',row['name'],row['ext_id'])
    print(f'imported {count} woredas to the database')
    return
    
if get_complete_dataframe_to_import() is not None:
    #define the columns of shape file to be imported code and name for the woreda and zone code
    cols_woreda=['ext_id','name','adm1']
    cols=['id_adm3','name_adm3','id_adm2']
    
    
    dataframe=get_complete_dataframe_to_import()
    etl_adm2(dataframe,cols,cols_woreda)
else:
    print('please check your shape file and path')