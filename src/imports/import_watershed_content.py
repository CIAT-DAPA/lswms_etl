from mongoengine import *
from ormWP import Typecontent
from ormWP import Wscontent
from ormWP import Waterpoint
from ormWP import Watershed

import os, sys
from datetime import datetime
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from parameters.get_connection import *
from parameters.get_file import *

connect(host=get_mongo_conn_str())

data = pd.read_csv(get_profiles_watershed_file(), delimiter=";",encoding='latin-1')
seasons = Typecontent.objects.get(name='seasons')
water_sources = Typecontent.objects.get(name='water sources')
zone_overview = Typecontent.objects.get(name='zone overview')

def update_wsc_water_source(watershed, ws1, ws2, ws3, ws4,ws5,ws6):
    # Verificar si ya existe un documento Wscontent para el tipo de contenido "general" y el Waterpoint actual
    existing_wpc_water_source = Wscontent.objects(type=water_sources, watershed=watershed).first()

    if existing_wpc_water_source:
        existing_wpc_water_source.content['values'][0]['ws1'] = ws1
        existing_wpc_water_source.content['values'][1]['ws2'] = ws2
        existing_wpc_water_source.content['values'][2]['ws3'] = ws3
        existing_wpc_water_source.content['values'][3]['ws4'] = ws4
        existing_wpc_water_source.content['values'][4]['ws5'] = ws5
        existing_wpc_water_source.content['values'][5]['ws6'] = ws6
        existing_wpc_water_source.content['trace']['updated'] = datetime.now()
        existing_wpc_water_source.save()
    else:
        # Si no existe un documento Wpcontent, crea uno nuevo
        trace = {
            "created": datetime.now(),
            "updated": datetime.now(),
            "enable": True
        }
        languages = {
            "spanish": "es",
            "english": "en",
            "amharic": "am"
        }
        content_data = {
            "title":water_sources.name,
            "type":"int",
            "values": [{"ws1": ws1},{"ws2": ws2},{"ws3": ws3},{"ws4": ws4},{"ws5": ws5},{"ws6": ws6}],
            "trace": trace,
            "languages": languages
        }

        wsc = Wscontent(
            content=content_data,
            watershed=watershed,
            type=water_sources,
        )
        wsc.save()

def update_wsc_seasons(watershed, s1, s2, s3, s4,s1m,s2m,s3m,s4m):
    # Verificar si ya existe un documento Wscontent para el tipo de contenido "general" y el Waterpoint actual
    existing_wpc_season = Wscontent.objects(type=seasons, watershed=watershed).first()

    if existing_wpc_season:
        existing_wpc_season.content['values'][0]['s1'] = s1
        existing_wpc_season.content['values'][1]['s2'] = s2
        existing_wpc_season.content['values'][2]['s3'] = s3
        existing_wpc_season.content['values'][3]['s4'] = s4
        existing_wpc_season.content['values'][4]['s1m'] = s1m
        existing_wpc_season.content['values'][5]['s2m'] = s2m
        existing_wpc_season.content['values'][6]['s3m'] = s3m
        existing_wpc_season.content['values'][7]['s4m'] = s4m
        existing_wpc_season.content['trace']['updated'] = datetime.now()
        existing_wpc_season.save()
    else:
        # Si no existe un documento Wpcontent, crea uno nuevo
        trace = {
            "created": datetime.now(),
            "updated": datetime.now(),
            "enable": True
        }
        languages = {
            "spanish": "es",
            "english": "en",
            "amharic": "am"
        }
        content_data = {
            "title":seasons.name,
            "type":"int",
            "values": [{"s1": s1},{"s2":s2},{"s3": s3},{"s4": s4},{"s1m": s1m},{"s2m": s2m},{"s3m": s3m},{"s4m": s4m}],
            "trace": trace,
            "languages": languages
        }

        wss = Wscontent(
            content=content_data,
            watershed=watershed,
            type=seasons,
        )
        wss.save()

def update_wsc_zone_overview(watershed, topography,hidrology,demography):
    # Verificar si ya existe un documento Wscontent para el tipo de contenido "general" y el Waterpoint actual
    existing_wpc_zone_overview = Wscontent.objects(type=zone_overview, watershed=watershed).first()

    if existing_wpc_zone_overview:
        existing_wpc_zone_overview.content['values'][0]['topography'] = topography
        existing_wpc_zone_overview.content['values'][1]['hidrology'] = hidrology
        existing_wpc_zone_overview.content['values'][2]['demography'] = demography
        existing_wpc_zone_overview.content['trace']['updated'] = datetime.now()
        existing_wpc_zone_overview.save()
    else:
        # Si no existe un documento Wpcontent, crea uno nuevo
        trace = {
            "created": datetime.now(),
            "updated": datetime.now(),
            "enable": True
        }
        languages = {
            "spanish": "es",
            "english": "en",
            "amharic": "am"
        }
        content_data = {
            "title":zone_overview.name,
            "type":"string",
            "values": [{"topography": topography},{"hidrology":hidrology},{"demography": demography}],
            "trace": trace,
            "languages": languages
        }

        wsz = Wscontent(
            content=content_data,
            watershed=watershed,
            type=zone_overview,
        )
        wsz.save()

for index, row in data.iterrows():
    watershed = Watershed.objects.get(name=str(row['watershed']))
    ws1 = row["watersource1"]       
    ws2 = row["watersource2"]       
    ws3 = row["watersource3"]       
    ws4 = row["watersource4"]       
    ws5 = row["watersource5"]       
    ws6 = row["watersource6"]   
    s1 = row["season1"]       
    s2 = row["season2"]       
    s3 = row["season3"]       
    s4 = row["season4"]      
    s1m = row["s1months"]       
    s2m = row["s2months"]       
    s3m = row["s3months"]       
    s4m = row["s4months"]       
    topography=row['topography']
    hidrology=row['hidrology']
    demography=row['demography']
    update_wsc_water_source(watershed, ws1, ws2, ws3, ws4,ws5,ws6)
    update_wsc_seasons(watershed, s1, s2, s3, s4,s1m,s2m,s3m,s4m)
    update_wsc_zone_overview(watershed, topography,hidrology,demography)
    # Verificar si ya existe un documento Wpcontent para el tipo de contenido "gender" y el Waterpoint actual
    existing_wpc_water_source = Wscontent.objects(type=water_sources, watershed=watershed).first()
    if not existing_wpc_water_source:
        ws1 = row["watersource1"]       
        ws2 = row["watersource2"]       
        ws3 = row["watersource3"]       
        ws4 = row["watersource4"]       
        ws5 = row["watersource5"]       
        ws6 = row["watersource6"]       
        trace = {
            "created": datetime.now(),
            "updated": datetime.now(),
            "enable": True
        }
        languages = {
            "spanish": "es",
            "english": "en",
            "amharic": "am"
        }
        content_data = {
            "title":water_sources.name,
            "type":"int",
            "values": [{"ws1": ws1},{"ws2": ws2},{"ws3": ws3},{"ws4": ws4},{"ws5": ws5},{"ws6": ws6}],
            "trace": trace,
            "languages": languages
        }


        wsc = Wscontent(
            content=content_data,
            watershed=watershed,
            type=water_sources,
        )
        wsc.save()


    existing_wpc_season = Wscontent.objects(type=seasons, watershed=watershed).first()
    if not existing_wpc_season:
        s1 = row["season1"]       
        s2 = row["season2"]       
        s3 = row["season3"]       
        s4 = row["season4"]      
        s1m = row["s1months"]       
        s2m = row["s2months"]       
        s3m = row["s3months"]       
        s4m = row["s4months"]      
        trace = {
            "created": datetime.now(),
            "updated": datetime.now(),
            "enable": True
        }
        languages = {
            "spanish": "es",
            "english": "en",
            "amharic": "am"
        }
        content_data = {
            "title":seasons.name,
            "type":"int",
            "values": [{"s1": s1},{"s2":s2},{"s3": s3},{"s4": s4},{"s1m": s1m},{"s2m": s2m},{"s3m": s3m},{"s4m": s4m}],
            "trace": trace,
            "languages": languages
        }


        wss = Wscontent(
            content=content_data,
            watershed=watershed,
            type=seasons,
        )
        wss.save()

    existing_wpc_zone_overview = Wscontent.objects(type=zone_overview, watershed=watershed).first()
    if not existing_wpc_zone_overview:
        topography=row['topography']
        hidrology=row['hidrology']
        demography=row['demography']
        trace = {
            "created": datetime.now(),
            "updated": datetime.now(),
            "enable": True
        }
        languages = {
            "spanish": "es",
            "english": "en",
            "amharic": "am"
        }
        content_data = {
            "title":zone_overview.name,
            "type":"int",
            "values": [{"topography": topography},{"hidrology":hidrology},{"demography": demography}],
            "trace": trace,
            "languages": languages
        }


        wsz = Wscontent(
            content=content_data,
            watershed=watershed,
            type=zone_overview,
        )
        wsz.save()