'''  This etl script imports zone administration of Ethioipia from a shape file for a selected zone. 
     The script imports connection string , geo-dataframe of the shaapefile and columns as parmeter to filter.  
     etl_adm1() is the function
   '''                          
import os
import sys
from mongoengine import connect
import geopandas as gpd
from datetime import datetime
from ormWP.models.adm1 import Adm1
from conn_str import conn_str
from data_import import  shp_import

#connection string
try:
    connect(host=conn_str())
    print('connection established')
except:
    print('please, check the connection string or if the database is started')
    sys.exit()

def etl_adm1(gdf,cols,zone):
    #filter zone
    gdf=gdf[gdf['name_adm2']==zone]
    #drop the duplicated row
    gdf=gdf[cols].drop_duplicates()
    cols_zone=['ext_id','name']
    gdf.columns=cols_zone
    count=0
    print('---------------------')
    for index, row in gdf.iterrows():
        if not Adm1.objects(ext_id=str(row['ext_id'])):
            traced_list = [
                {"created": datetime.now(), "updated": datetime.now(), "enabled": True}
            ]

            print('importing',row['name'],row['ext_id'])
            adm1=Adm1(name=row['name'],ext_id=str(row['ext_id']),traced=traced_list)
            adm1.save()
            count+=1
        else:
            print('not imported',row['name'],row['ext_id'])
    print(f'imported {count} records to the database')
    return

#check the shape file and excute the import to monog db 
if shp_import():
    #define the columns of shape file to be imported code and name for the woreda and zone code
    gdf,columns=shp_import()
    zone=columns[0]
    cols_adm1=columns[1]
    #call the adm1 function with arguments geo-dtaframe, columns to be filterd and zone 
    etl_adm1(gdf,cols_adm1,zone)
else:
    print('please check your shape file path and columns are correct')
