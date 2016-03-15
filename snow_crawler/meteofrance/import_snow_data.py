import csv
import io
import requests
import json
import sys
import logging
import pandas as pd

from geojson import Point
from datetime import date, datetime, timedelta
from sys import argv
from pymongo import MongoClient, DESCENDING


def get_snow_data(url):
#Retrive data from a given url and stock it in a DataFrame
    response =  requests.get(url)
    try:
        content_disposition = response.headers['content-disposition']
    except KeyError:
        content_disposition = None

    if content_disposition != 'attachment':
        print 'No data found at this url: \n {0}'.format(url)
        raise ValueError('wrong data')


    web_data = response.text.decode('utf-8').splitlines()

    snow_data = []
    web_csv = csv.reader(web_data, delimiter = ';')
    header = next(web_csv)[0:-2]

    for row in web_csv:
        n = len(row)
        for ix in range(0, n):
            if row[ix] == 'mq' or row[ix] == '/':
                row[ix] = None
        snow_data.append(row[0:-1])

    snow_df = pd.DataFrame(snow_data, columns = header)

    if len(snow_df) < 2:
        print 'No data available at {0}'.format(url)
        return 1

    return snow_df

def to_float(x):
    if x is None:
        y = None
    else:
        y = float(x)
    return y

def to_int(x):
        if x != x or x is None:
            y = None
        else:
            y = int(x)
        return y

def to_string(x):
    if x is None:
        y = None
    else:
        y = str(x)
    return y

def convert_df_columns(snow_df, schema):
#convert each df columns to the right type using the schema and convert the df
#to json
    for col in snow_df.columns:
        col_type = schema[col]
        if col_type == 'float':
            snow_df[col] = snow_df[col].apply(lambda x: to_float(x))
        elif col_type == 'int':
            snow_df[col] = snow_df[col].apply(lambda x: to_int(x))
        elif col_type == 'string':
            snow_df[col] = snow_df[col].apply(lambda x: to_string(x))

    snow_json = []
    n = len(snow_df)
    for ix in range(0,n):
        snow_json.append(snow_df.ix[ix,:].to_dict())

    return snow_json

def get_db(host, port, database):
#return a connection with the database, create it if the database does not exist
    client = MongoClient(host, port)
    db = client[database]
    return db

def send_data(db, collection, data):
    db = db[collection]
    insert_many_result = db.insert_many(data)
    documents_inserted = len(insert_many_result.inserted_ids)
    print 'insertion finished, {0} documents inserted'.format(documents_inserted)
    return 0

def send_station_data(url, host, port, database, collection):
    """send station info data, this has to be done only once has those
        informations does not change"""
    response =  requests.get(url)
    try:
        content_disposition = response.headers['content-disposition']
    except KeyError:
        content_disposition = None

    if content_disposition != 'attachment':
        print 'No data found at this url: \n {0}'.format(url)
        raise ValueError('wrong data')


    web_data = response.text.decode('utf-8').splitlines()

    stations = []
    web_csv = csv.reader(web_data, delimiter = ',')
    header = next(web_csv)

    for row in web_csv:
        station_dic = dict(zip(header, row))
        lat = float(station_dic['Latitude'])
        del station_dic['Latitude']
        lon = float(station_dic['Longitude'])
        del station_dic['Longitude']
        alt = float(station_dic['Altitude'])
        station_dic['station_geojson'] = Point((lon, lat, alt))
        stations.append(station_dic)

    #send data
    db = get_db(host, port, database)
    send_data(db, collection, stations)
    return 0

def send_daily_data(date_to_send, url, schema, host, port,
                    database, collection):
#If already an entry in the database for that day
#do not write anything
    db = get_db(host, port, database)
    db = db[collection]
    dates = [d[0:8] for d in db.distinct('date')]

    if date_to_send is None:
        #Generate URL for  yesterday
        yesterday = date.today() - timedelta(1)
        date_to_send = yesterday.strftime('%Y%m%d')

    if date_to_send in dates:
        print 'database already have data for {0}'.format(date_to_send)
        return 1

    url = url.replace('########', date_to_send)

    try:
        daily_snow_df = get_snow_data(url)
    except ValueError as err:
        return 1

    daily_snow_data = convert_df_columns(daily_snow_df, schema)

    db = get_db(host, port, database)
    send_data(db, collection, daily_snow_data)
    return 0

def send_historical_data(month, year, url, schema, host, port,
                         database, collection):
        db = get_db(host, port, database)
        db = db[collection]

        url = url.replace('#YYYY#', year).replace('#MM#', month)

        try:
            hist_snow_df = get_snow_data(url)
        except ValueError as err:
            return 1

        hist_snow_data = convert_df_columns(hist_snow_df, schema)

        db = get_db(host, port, database)
        send_data(db, collection, hist_snow_data)
        return 0

def daterange(start, end):
    for n in range(int((end-start).days + 1)):
        yield (start + timedelta(n)).strftime('%Y%m%d')

def month_year_iter( start_month, start_year, end_month, end_year ):
    ym_start= 12*start_year + start_month - 1
    ym_end= 12*end_year + end_month - 1
    for ym in range( ym_start, ym_end ):
        y, m = divmod( ym, 12 )
        year = str(y)
        if m+1 < 10:
            month = '0'+str(m+1)
        else:
            month = str(m+1)
        yield year, month


def main():

    #read conf files
    with open('conf.json') as f:
        data = json.load(f)

    db = get_db(data['host'], data['port'], data['database'])
    station = db.station.find()

    if station.count() > 0:
        logging.log(logging.INFO,
                    "Station database already here")
    else:
        send_station_data(data['station_url'], data['host'],
                          data['port'], data['database'],
                          data['station_collection'])

    #read schema
    schema = {}
    with open('schema.csv') as f:
         for row in f:
             name, type = row.split()
             schema[name] = type

    #if dataset is empty send all historical data:
    start_year, start_month = 2010, 01
    now = datetime.now()
    end_year, end_month = int(datetime.strftime(now,'%Y')), int(datetime.strftime(now,'%m'))

    db = get_db(data['host'], data['port'], data['database'])
    collection = data['meteo_collection']
    query = db[collection].count()


    if query ==  0:
        for y, m in month_year_iter(start_month, start_year, end_month, end_year):
            try:
                print 'send {0}-{1} data'.format(m, y)
                send_historical_data(m, y, data['meteo_history_url'],
                                schema, data['host'], data['port'],
                                data['database'], data['meteo_collection'])
            except:
                print 'problem with data {0}-{1}'.format(m, y)
    args = sys.argv
    if len(args) == 1:
        send_daily_data(None, data['meteo_url'], schema,
                        data['host'], data['port'],
                        data['database'], data['meteo_collection'])
    elif len(args) == 2:
        date_to_send = args[1]
        send_daily_data(date_to_send, data['meteo_url'], schema,
                        data['host'], data['port'],
                        data['database'], data['meteo_collection'])
    elif len(args) == 3:
        start_date = datetime.strptime(args[1], '%Y%m%d')
        end_date = datetime.strptime(args[2], '%Y%m%d')
        for d in daterange(start_date, end_date):
            send_daily_data(d, data['meteo_url'], schema,
                            data['host'], data['port'],
                            data['database'], data['meteo_collection'])



if __name__ == '__main__':
    main()
