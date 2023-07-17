#script checks the shapefile 

import os

#data path to the shape file
def get_shp_file_import():
    data_path='D:\ETL\data'
    shape_dir='admin_levels_ws_id'
    data=os.path.join(data_path,shape_dir)
    file_name='admin_levels_ws_id'
    file_shp=os.path.join(data,file_name +'.shp')
    if os.path.exists(file_shp):
        return(file_shp)

def get_watershed_file():
    data_path='D:\ETL\data'
    file_name='w_shade'
    file_csv=os.path.join(data_path,file_name +'.csv')
    if os.path.exists(file_csv):
        return(file_csv)
