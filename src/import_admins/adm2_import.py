'''  This etl script imports woreda or districl level administration of Ethioipia from a shape file for a selected zone. 
     The script imports connection string , geo-dataframe of the shaapefile and columns as parmeter to filter.  
     etl_adm2() is the function
   '''

import os
import sys
from mongoengine import connect
import geopandas as gpd
from datetime import datetime
from conn_str import conn_str
from data_import import  shp_import
from ormWP.models.adm1 import Adm1
from ormWP.models.adm2 import Adm2

#check the connection to the database
try:
    connect(host=conn_str())
    print('connection established')

except:
    print('plese check your database connection')
    sys.exit()



def etl_adm2(gdf,cols,zone):
    gdf=gdf[gdf['name_adm2']==zone]
    gdf=gdf[cols].drop_duplicates()
    cols_woreda=['ext_id','name','adm1']
    gdf.columns=cols_woreda  
    count=0
    print('---------------------')
    for index, row in gdf.iterrows():
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
    print(f'imported {count} records to the database')
    return    


if shp_import():
    #define the columns of shape file to be imported code and name for the woreda and zone code
    gdf,columns=shp_import()
    zone=columns[0]
    cols_adm2=columns[2]
    #call the adm1 function with arguments geo-dtaframe, columns to be filterd and zone 
    etl_adm2(gdf,cols_adm2,zone)
else:
    print('please check your shape file and path')

