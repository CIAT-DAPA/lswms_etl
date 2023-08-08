from mongoengine import *
import os, sys
from datetime import datetime
import pandas as pd
from ormWP import Typecontent

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from parameters.get_connection import *
from parameters.get_file import *

def save_contents_to_database(contents):
    connect(host=get_mongo_conn_str())

    for element in contents:
        # Check if the type of content already exists in the database
        existing_type = Typecontent.objects(name=element).first()

        # If the type of content doesn't exist, create and save a new Typecontent object
        if not existing_type:
            type = Typecontent(name=element)
            type.save()

# Llamada a la función con el contenido deseado
contents = ["gender", "challenges", "agriculture context", "livehood", "general","seasons","water sources","zone overview"]
save_contents_to_database(contents)
