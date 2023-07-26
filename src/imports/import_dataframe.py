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
        # Apply coordinate systems
        adminlevels = gdf.to_crs("EPSG:4326")
        adminlevels = adminlevels[["id_adm1", "id_adm2", "id_adm3", "id_adm4", "name_adm1", "name_adm2",
                                   "name_adm3", "name_adm4", "ws_id", "geometry"]]
        return adminlevels
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
        
        # Define additional points
        proof=Point(38.318161, 12.513699)
        proof2=Point(36.294721, 9.811474)
        proof3=Point(36.083094, 11.060541)
        proof4=Point(36.686477, 8.316135)
        proof5=Point(36.608792, 11.331677)
        proof6=Point(37.836216, 10.630065)
        proof7=Point(36.896502, 9.886837)
        proof8=Point(35.108797, 9.286759)
        proof9=Point(37.942499, 11.382111)
        proof10=Point(35.907559, 8.595833)
        
        # Assign the additional points to the GeoDataFrame
        geodata['geometry'][0] = proof
        geodata['geometry'][1] = proof2
        geodata['geometry'][2] = proof3
        geodata['geometry'][3] = proof4
        geodata['geometry'][4] = proof5
        geodata['geometry'][5] = proof6
        geodata['geometry'][6] = proof7
        geodata['geometry'][7] = proof8
        geodata['geometry'][8] = proof9
        geodata['geometry'][9] = proof10
        
        # Perform the spatial join
        totaldataframe = gdp.sjoin(geodata, get_dataframe_shp(), how="left", predicate="within")
        proof11='hola soy nuevo 4.0'
        totaldataframe['name_adm2'][0] = proof11
        proof12='hola soy nuevo 5.0'
        totaldataframe['name_adm3'][0] = proof12
        proof13='hola soy nuevo 6.0'
        totaldataframe['name_adm4'][0] = proof13
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


