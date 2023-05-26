from mongoengine import *

# Region
class Region(Document):
    name = StringField(required=True)

# Zone
class Zone(Document):
    name = StringField(required=True)
    region = ReferenceField(Region)

# Woreda
class Woreda(Document):
    name = StringField(required=True)  
    zone = ReferenceField(Zone)

# Kebele
class Kebele(Document):
    name = StringField(required=True)
    woreda = ReferenceField(Woreda)


class HistoricalData(Document):
    date = StringField(required=True)
    rain = FloatField(required=True)
    evaporation = FloatField(required=True)
    depth = FloatField(required=True)
    scaled_depth = FloatField(required=True)


class WaterPoints(Document):
    
    kebele = ReferenceField(Kebele)
    latitude=FloatField(required=True)
    longitude=FloatField(required=True)
    name = StringField(required=True)
    waterPointArea=FloatField(required=True)
    waterSharedArea=FloatField(required=True)
    atributes = ListField(required= True)



