'''  This etl script cleans the shp file import process and returns geo-datafram and columns that are used
     by the Adm1_import, Adm2_import and Adm3_import   
'''


import os
import numpy as np
import geopandas as gpd

#data path to the shape file
def shp_import():
    #path to the shape file
    data_path='../data'
    shape_dir='eth_adm_csa_bofedb_2021_shp'
    data=os.path.join(data_path,shape_dir)
    file_name='admin_levels_ws_id'
    #check if the file in the path exists
    file_shp=os.path.join(data,file_name +'.shp')
    if os.path.exists(file_shp):
        cols_adm1=['id_adm2','name_adm2']
        cols_adm2=['id_adm3','name_adm3','id_adm2']
        cols_adm3=['id_adm4','name_adm4','id_adm3','ws_id']
        #read the shape file to a geo-data frame
        gdf=gpd.read_file(file_shp)
        #filter only unique columns since there are repeated columns
        list_cols=np.unique(cols_adm1+cols_adm2+cols_adm3)
        list_err=[]
        #check the input columns are in the geo-dataframe column index 
        for col in list_cols:
            if col not in gdf.columns:
                list_err.append(col)
        if len(list_err)==0:
            zone='Borena'
            cols=[zone,cols_adm1,cols_adm2,cols_adm3]

            return(gdf,cols)
    
