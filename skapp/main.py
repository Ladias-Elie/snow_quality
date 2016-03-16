import os
import json
import re

from flask import Flask, render_template, request, redirect
from pymongo import MongoClient
from datetime import datetime
from geojson import Point, Feature, dump
from bson import json_util

app = Flask(__name__)

MONGO_URL = 'ec2-54-93-47-134.eu-central-1.compute.amazonaws.com:27017'
client = MongoClient(MONGO_URL)

@app.route("/")
def show_map():
    return render_template('map.html')

@app.route("/prediction")
def get_prediction():
    today = datetime.today().strftime('%Y%m%d')

    db = client['meteo_neige']

    query = {'date':str(today)}
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

    return str(json.dumps(geojson_data, default=json_util.default))

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
