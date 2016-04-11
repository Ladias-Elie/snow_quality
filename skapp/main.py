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
    db = client['app']
    result = db['prediction'].find({},{'_id':False})
    return str(json.dumps(list(result),  default=json_util.default))
    
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
