import os, sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from parameters.get_connection import *
from parameters.get_file import *
from mongoengine import *
import pandas as pd
import geopandas as gdp
from shapely.geometry import Point
import psycopg2
import numpy as np

def get_dataframe():
    conn = None
    try:
        conn = psycopg2.connect(get_postres_conn_str())
        cur = conn.cursor()
        
    except Exception as ex:
        print('Ocurrió un error al conectarse a la base de datos:')
        print(str(ex))
    
    sql="select name,gid,grid_code,x_coord,y_coord,wp_km2,uid,wshed_km2,country from locations where country='Ethiopia';"
    cur.execute(sql)
    results = cur.fetchall()


    # Crear un DataFrame con los resultados
    df = pd.DataFrame(results, columns=['name', 'gid', 'grid_code', 'x_coord', 'y_coord', 'wp_km2', 'uid', 'wshed_km2', 'country'])
    #read csv files with 
    return df

def get_dataframe_with_watershed():
    try:
        dataframewh = pd.read_csv(get_watershed_file())
        # Filter dataframe to get waterpoints with activity
        df_filtered = get_dataframe()[get_dataframe()['name'].isin(dataframewh['name'])]
        df_filtered = df_filtered.merge(dataframewh, on='name')
        return df_filtered
    except Exception as e:
        # Create an "error" folder if it doesn't exist
        error_folder = os.path.join(os.path.dirname(__file__), '..', 'error')
        os.makedirs(error_folder, exist_ok=True)

        # Log the error message to a file
        error_log_file = os.path.join(error_folder, 'error_dataframe_log.txt')
        with open(error_log_file, 'a') as f:
            f.write(str(e) + '\n')
        return None
    
def get_dataframe_shp():
    try:
        gdf = gdp.read_file(get_shp_file_import())
        columns_to_delete = ['OBJECTID', 'COUNT', 'T_CODE','KK_CODE','UK_NAME','UK_CODE','KK_NAME','UK_ID','Shape__Are','Shape__Len','GlobalID','T_NAME']
# Use the 'drop' method to delete the specified columns
        gdf = gdf.drop(columns=columns_to_delete)
        colums_names = {
            'R_NAME': 'name_adm1',
            'R_CODE': 'id_adm1',
            'Z_NAME': 'name_adm2',
            'Z_CODE': 'id_adm2',
            'W_NAME': 'name_adm3',
            'W_CODE': 'id_adm3',
            'RK_NAME': 'name_adm4',
            'RK_CODE': 'id_adm4'
        }

# Use the 'rename' method to change column names
        # Apply coordinate systems
        adminlevels = gdf.to_crs("EPSG:4326")
        adminlevels = adminlevels.rename(columns=colums_names)

        cols_adm1=['id_adm2','name_adm2']
        cols_adm2=['id_adm3','name_adm3','id_adm2']
        cols_adm3=['id_adm4','name_adm4','id_adm3']
        #filter only unique columns avoid repeated columns
        list_cols=np.unique(cols_adm1+cols_adm2+cols_adm3)
        list_err=[]
        #check the input columns are in the geo-dataframe column index
        for col in list_cols:
            if col not in adminlevels.columns:
                list_err.append(col)
        if len(list_err)==0:
            cols=[cols_adm1,cols_adm2,cols_adm3]
            adminleve1s=adminlevels[list(np.append(list_cols,"geometry"))]
            return adminleve1s,cols
    except Exception as e:
        # Create an "error" folder if it doesn't exist
        error_folder = os.path.join(os.path.dirname(__file__), '..', 'error')
        os.makedirs(error_folder, exist_ok=True)

        # Log the error message to a file
        error_log_file = os.path.join(error_folder, 'error_dataframeshp_log.txt')
        with open(error_log_file, 'a') as f:
            f.write(str(e) + '\n')
        return None

def get_geo_points():
    try:
        df_with_ws = get_dataframe_with_watershed()
        points = [Point(xy) for xy in zip(df_with_ws["x_coord"], df_with_ws["y_coord"])]
        return points
    except Exception as e:
        # Create an "error" folder if it doesn't exist
        error_folder = os.path.join(os.path.dirname(__file__), '..', 'error')
        os.makedirs(error_folder, exist_ok=True)

        # Log the error message to a file
        error_log_file = os.path.join(error_folder, 'error_geopoints_log.txt')
        with open(error_log_file, 'a') as f:
            f.write(str(e) + '\n')
        return None



def get_complete_dataframe_to_import():
    try:
        geodata = gdp.GeoDataFrame(get_dataframe_with_watershed(), geometry=get_geo_points(), crs="EPSG:4326")
       
        
        # Perform the spatial join
        gdf,cols=get_dataframe_shp()
        totaldataframe = gdp.sjoin(geodata, gdf, how="left", predicate="within")

        totaldataframe.replace(np.nan, None, inplace=True)
        return totaldataframe
    except Exception as e:
        # Create an "error" folder if it doesn't exist
        error_folder = os.path.join(os.path.dirname(__file__), '..', 'error')
        os.makedirs(error_folder, exist_ok=True)

        # Log the error message to a file
        error_log_file = os.path.join(error_folder, 'error_total_dataframe_log.txt')
        with open(error_log_file, 'a') as f:
            f.write(str(e) + '\n')
        return None

