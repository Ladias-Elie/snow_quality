#script wich take predictio data and make it look right for the flask app
import re

from pymongo import MongoClient
from datetime import datetime, timedelta
from geojson import Point, Feature, dump

MONGO_URL = 'ec2-54-93-47-134.eu-central-1.compute.amazonaws.com:27017'
client = MongoClient(MONGO_URL)

#Prediction are only available after 5 in the morning
if datetime.now().hour < 5:
    date = datetime.today() - timedelta(1)
else:
    date = datetime.today()

date = date.strftime('%Y%m%d')

db = client['meteo_neige']
query = {'date':str(date)}
result = db['prediction'].find(query)

dep_latlon_list = list(set([x['dep_latlon'] for x in result]))

geojson_data = []
for latlon in dep_latlon_list:

    #get trip start info
    query = {'dep_latlon':str(latlon)}
    depart = db['prediction'].find_one(query)
    lat, lon = [float(x) for x in re.findall("\d*\.\d*", latlon)]
    dep_name = depart['dep_name']

    #get trip info
    trips_list = []
    trips = db.prediction.find(query)
    for x in trips:
        trip_info = {'nom': x['nom'],
                     'denivele': x['denivele'],
                     'skiability': x['skiabiliy'],
                     'orientation': x['orientation']}
        trips_list.append(trip_info)

    dep_point = Point((lon,lat))
    dep_properties = {'name':dep_name,
                      'trips': trips_list}
    daily_condition = Feature(geometry=dep_point,
                              properties=dep_properties)
    geojson_data.append(daily_condition)

app_db = client['app']
app_db['prediction'].remove()
app_db['prediction'].insert_many(geojson_data)
