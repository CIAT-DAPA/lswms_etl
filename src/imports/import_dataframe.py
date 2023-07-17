import os, sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from parameters.get_connection import *
from parameters.get_file import *
from mongoengine import *
import pandas as pd
import geopandas as gdp
from shapely.geometry import Point
import psycopg2

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
    dataframewh = pd.read_csv(get_watershed_file())
    #filter dataframe to get waterpoints with activity
    df_filtered = get_dataframe()[get_dataframe()['name'].isin(dataframewh['name'])]
    df_filtered = df_filtered.merge(dataframewh, on='name')
    return df_filtered
    
def get_dataframe_shp():
    gdf=gdp.read_file(get_shp_file_import())
    #apply coordinate systems
    adminleves = gdf.to_crs("EPSG:4326")
    adminleves = adminleves[["id_adm1","id_adm2","id_adm3","id_adm4","name_adm1","name_adm2","name_adm3","name_adm4","ws_id", "geometry"]]
    return adminleves

def get_geo_points():
    points = [Point(xy) for xy in zip(get_dataframe_with_watershed()["x_coord"], get_dataframe_with_watershed()["y_coord"])]
    return points

def get_complete_dataframe_to_import():
    
    geodata =gdp.GeoDataFrame(get_dataframe_with_watershed(), geometry=get_geo_points(), crs="EPSG:4326")
    proof=Point(38.318161,12.513699)
    proof2=(Point(36.294721,9.811474))
    proof3=Point(36.083094,11.060541)
    proof4=Point(36.686477,8.316135)
    proof5=Point(36.608792,11.331677)
    proof6=Point(37.836216,10.630065)
    proof7=Point(36.896502,9.886837)
    proof8=Point(35.108797,9.286759)
    proof9=Point(37.942499,11.382111)
    proof10=Point(35.907559,8.595833)
    
    geodata['geometry'][0]=proof
    geodata['geometry'][1]=proof2
    geodata['geometry'][2]=proof3
    geodata['geometry'][3]=proof4
    geodata['geometry'][4]=proof5
    geodata['geometry'][5]=proof6
    geodata['geometry'][6]=proof7
    geodata['geometry'][7]=proof8
    geodata['geometry'][8]=proof9
    geodata['geometry'][9]=proof10
    totaldataframe = gdp.sjoin(geodata, get_dataframe_shp(), how="left", predicate="within")
    
    

    return totaldataframe


