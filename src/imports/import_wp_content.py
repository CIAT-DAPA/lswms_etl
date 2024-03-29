from mongoengine import *
from ormWP import Typecontent
from ormWP import Wpcontent
from ormWP import Waterpoint
import os, sys
from datetime import datetime
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from parameters.get_connection import *
from parameters.get_file import *

connect(host=get_mongo_conn_str())

data = pd.read_csv(get_profiles_watershed_file(), delimiter=";",encoding='latin-1')
gender = Typecontent.objects.get(name='gender')
challengue = Typecontent.objects.get(name='challenges')
agriculture = Typecontent.objects.get(name='agriculture context')
livehood = Typecontent.objects.get(name='livehood')
general = Typecontent.objects.get(name='general')

# Itera sobre cada fila del DataFrame y guarda los datos "male" y "female" en documentos Wpcontent
# ... Código anterior ...

# Itera sobre cada fila del DataFrame y guarda los datos "male" y "female" en documentos Wpcontent
def update_wpc_gender(waterpoint, male, female):
    # Verificar si ya existe un documento Wpcontent para el tipo de contenido "gender" y el Waterpoint actual
    existing_wpc_gender = Wpcontent.objects(type=gender, waterpoint=waterpoint).first()

    if existing_wpc_gender:
        # Si el documento Wpcontent existe, actualiza los valores "male" y "female" y el campo "updated" en el diccionario "trace"
        existing_wpc_gender.content['values'][0]['male'] = male
        existing_wpc_gender.content['values'][1]['female'] = female
        existing_wpc_gender.content['trace']['updated'] = datetime.now()
        existing_wpc_gender.save()
    else:
        # Si no existe un documento Wpcontent, crea uno nuevo
        trace = {
            "created": datetime.now(),
            "updated": datetime.now(),
            "enable": True
        }
       
        content_data = {
            "title": gender.name,
            "type": "icon-y",
            "values": [{"male": male}, {"female": female}],
            "trace": trace,
            "language": "en" 
        }

        wpc = Wpcontent(
            content=content_data,
            waterpoint=waterpoint,
            type=gender,
        )
        wpc.save()


def update_wpc_challenge_agriculture(waterpoint, challenge1, challenge4):
    # Verificar si ya existe un documento Wpcontent para el tipo de contenido "challenge" y el Waterpoint actual
    existing_wpc_challenge  = Wpcontent.objects(
        Q(type=challengue) & Q(waterpoint=waterpoint) & Q(content__title=f"{challengue.name} agriculture")
    ).first()

    if existing_wpc_challenge:
        # Si el documento Wpcontent existe, actualiza los datos de los desafíos y el campo "updated" en el diccionario "trace"
        existing_wpc_challenge.content['values'][0]['1'] = challenge1
        existing_wpc_challenge.content['values'][1]['2'] = challenge4
        existing_wpc_challenge.content['trace']['updated'] = datetime.now()
        existing_wpc_challenge.save()
    else:
        # Si no existe un documento Wpcontent, crea uno nuevo
        trace = {
            "created": datetime.now(),
            "updated": datetime.now(),
            "enable": True
        }


        content_data = {
            "title":f"{challengue.name} livestock",
            "type":"icon-x",
            "values": [{"1": challenge1}, {'2': challenge4}],
            "trace": trace,
            "language": "en" 
        }

        wpcch = Wpcontent(
            content=content_data,
            waterpoint=waterpoint,
            type=challengue,
        )
        wpcch.save()

def update_wpc_challenge_crops(waterpoint, challenge2, challenge3):
    # Verificar si ya existe un documento Wpcontent para el tipo de contenido "challenge" y el Waterpoint actual
    existing_wpc_challenge  = Wpcontent.objects(
        Q(type=challengue) & Q(waterpoint=waterpoint) & Q(content__title=f"{challengue.name} crops")
    ).first()

    if existing_wpc_challenge:
        # Si el documento Wpcontent existe, actualiza los datos de los desafíos y el campo "updated" en el diccionario "trace"
        existing_wpc_challenge.content['values'][0]['1'] = challenge2
        existing_wpc_challenge.content['values'][1]['2'] = challenge3
        existing_wpc_challenge.content['trace']['updated'] = datetime.now()
        existing_wpc_challenge.save()
    else:
        # Si no existe un documento Wpcontent, crea uno nuevo
        trace = {
            "created": datetime.now(),
            "updated": datetime.now(),
            "enable": True
        }


        content_data = {
            "title":f"{challengue.name} crops",
            "type":"icon-x",
            "values": [{"1": challenge2}, {'2': challenge3}],
            "trace": trace,
            "language": "en" 
        }

        wpcch = Wpcontent(
            content=content_data,
            waterpoint=waterpoint,
            type=challengue,
        )
        wpcch.save()

def update_wpc_agriculture(waterpoint, crop1, crop2, crop3):
    # Verificar si ya existe un documento Wpcontent para el tipo de contenido "agriculture" y el Waterpoint actual
    existing_wpc_agriculture = Wpcontent.objects(
        Q(type=agriculture) & Q(waterpoint=waterpoint) & Q(content__title=f"{agriculture.name} crops")
    ).first()

    if existing_wpc_agriculture:
        # Si el documento Wpcontent existe, actualiza los datos de agricultura y el campo "updated" en el diccionario "trace"
        existing_wpc_agriculture.content['values'][0]['crop'] = crop1
        existing_wpc_agriculture.content['values'][1]['crop2'] = crop2
        existing_wpc_agriculture.content['values'][2]['crop3'] = crop3
        existing_wpc_agriculture.content['trace']['updated'] = datetime.now()
        existing_wpc_agriculture.save()
    else:
        # Si no existe un documento Wpcontent, crea uno nuevo
        trace = {
            "created": datetime.now(),
            "updated": datetime.now(),
            "enable": True
        }

        content_data = {
            "title":f"{agriculture.name} crops",
            "type":"icon-y",
            "values": [
                {"crop": crop1},
                {"crop2": crop2},
                {"crop3": crop3}
            ],
            "trace": trace,
            "language": "en" 
        }

        wpca = Wpcontent(
            content=content_data,
            waterpoint=waterpoint,
            type=agriculture,
        )
        wpca.save()

def update_wpc_agriculture_livestock(waterpoint, goat, sheep, cattle, camel, donkey):
    # Verificar si ya existe un documento Wpcontent para el tipo de contenido "agriculture" y el Waterpoint actual
    existing_wpc_agriculture_livestock = Wpcontent.objects(
        Q(type=agriculture) & Q(waterpoint=waterpoint) & Q(content__title=f"{agriculture.name} livestock")
    ).first()

    if existing_wpc_agriculture_livestock:
        # Si el documento Wpcontent existe, actualiza los datos de agricultura y el campo "updated" en el diccionario "trace"
        existing_wpc_agriculture_livestock.content['values'][0]['goat'] = goat
        existing_wpc_agriculture_livestock.content['values'][1]['sheep'] = sheep
        existing_wpc_agriculture_livestock.content['values'][2]['cattle'] = cattle
        existing_wpc_agriculture_livestock.content['values'][3]['camel'] = camel
        existing_wpc_agriculture_livestock.content['values'][4]['donkey'] = donkey
        existing_wpc_agriculture_livestock.content['trace']['updated'] = datetime.now()
        existing_wpc_agriculture_livestock.save()
    else:
        # Si no existe un documento Wpcontent, crea uno nuevo
        trace = {
            "created": datetime.now(),
            "updated": datetime.now(),
            "enable": True
        }

        content_data = {
            "title":f"{agriculture.name} livestock",
            "type":"icon-y",
            "values": [
                {"goat": goat} ,
                {"sheep": sheep},
                {"cattle": cattle} ,
                {"camel": camel} ,
                {"donkey": donkey} 
            ],
            "trace": trace,
            "language": "en" 
        }

        wpca = Wpcontent(
            content=content_data,
            waterpoint=waterpoint,
            type=agriculture,
        )
        wpca.save()

def update_wpc_livehood(waterpoint, liv, liv1):
    # Verificar si ya existe un documento Wpcontent para el tipo de contenido "livehood" y el Waterpoint actual
    existing_wpc_livehood = Wpcontent.objects(type=livehood, waterpoint=waterpoint).first()

    if existing_wpc_livehood:
        # Si el documento Wpcontent existe, actualiza los datos de medios de vida y el campo "updated" en el diccionario "trace"
        existing_wpc_livehood.content['values'][0]['1'] = liv
        existing_wpc_livehood.content['values'][1]['2'] = liv1
        existing_wpc_livehood.content['trace']['updated'] = datetime.now()
        existing_wpc_livehood.save()
    else:
        # Si no existe un documento Wpcontent, crea uno nuevo
        trace = {
            "created": datetime.now(),
            "updated": datetime.now(),
            "enable": True
        }

        content_data = {
            "title":livehood.name,
            "type":"icon-y",
            "values": [
                {"1": liv},
                {"2": liv1}
            ],
            "trace": trace,
            "language": "en" 
        }

        wpcl = Wpcontent(
            content=content_data,
            waterpoint=waterpoint,
            type=livehood,
        )
        wpcl.save()

def update_wpc_general(waterpoint, construction, owned, constructed, status,male,female):
    # Verificar si ya existe un documento Wpcontent para el tipo de contenido "general" y el Waterpoint actual
    existing_wpc_general = Wpcontent.objects(type=general, waterpoint=waterpoint).first()

    if existing_wpc_general:
        # Si el documento Wpcontent existe, actualiza los datos generales y el campo "updated" en el diccionario "trace"
        existing_wpc_general.content['values'][0]['construction'] = construction
        existing_wpc_general.content['values'][1]['owned'] = owned
        existing_wpc_general.content['values'][2]['constructed'] = constructed
        existing_wpc_general.content['values'][3]['status in dry season'] = status
        existing_wpc_general.content['trace']['updated'] = datetime.now()
        existing_wpc_general.save()
    else:
        # Si no existe un documento Wpcontent, crea uno nuevo
        trace = {
            "created": datetime.now(),
            "updated": datetime.now(),
            "enable": True
        }

        content_data = {
            "title":general.name,
            "type":"table",
            "values": [
               {"construction": construction},
               {"owned": owned},
               {"constructed": constructed},
               {"status in dry season": status},
               {"population": male+female},
               
                ],
            "trace": trace,
            "language": "en" 
        }

        wpcg = Wpcontent(
            content=content_data,
            waterpoint=waterpoint,
            type=general,
        )
        wpcg.save()

for index, row in data.iterrows():
    # Obtén el objeto Waterpoint que corresponde a esta fila (si el modelo se llama Waterpoint)
    waterpoint = Waterpoint.objects.get(ext_id=str(row['uid']))
    male = row["male"]       # Asignar los valores desde el DataFrame a las variables male y female
    female = row["female"] 
    temp = row['temp']
    tempmax = row['tempmax']
    tempmin = row['tempmin']
    precipitation = row['precipitation']
    challenge1 = row['challenge1']
    challenge2 = row['challenge2']
    challenge3 = row['challenge3']
    challenge4 = row['challenge4']
    crop1 = row['crop1']
    crop2 = row['crop2']
    crop3 = row['crop3']
    goat = row['goat']
    sheep = row['sheep']
    cattle = row['cattle']
    camel = row['camel']
    donkey = row['donkey']
    liv = row['livehood']
    liv1 = row['livehood1']
    construction = row['construction']
    owned = row['owned']
    constructed = row['constructed']
    status = row['status']
    update_wpc_general(waterpoint, construction, owned, constructed, status,male,female)
    update_wpc_livehood(waterpoint, liv, liv1)
    update_wpc_agriculture(waterpoint, crop1, crop2, crop3)
    update_wpc_agriculture_livestock(waterpoint, goat, sheep, cattle, camel, donkey) 
    update_wpc_challenge_agriculture(waterpoint, challenge1, challenge4)
    update_wpc_challenge_crops(waterpoint, challenge2, challenge3)
    update_wpc_gender(waterpoint, male, female)
    # Verificar si ya existe un documento Wpcontent para el tipo de contenido "gender" y el Waterpoint actual
