# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import re
import pymongo
import logging

from datetime import datetime
from geojson import Point


class CleanerPipeline(object):
    """Clean scrapped data so they are ready to be integrated into mongo database"""
    def convert_date(self, date):
        month_dict = {'janvier':'january',
                      'fevrier':'february',
                      'mars':'march',
                      'avril':'april',
                      'mai':'may',
                      'juin':'june',
                      'juillet':'july',
                      'aout':'august',
                      'septembre':'september',
                      'octobre':'october',
                      'novembre':'november',
                      'decembre':'december'}

        french_month = date.split()[1]
        english_month = month_dict[french_month]
        english_date = date.replace(french_month, english_month)
        python_date = datetime.strptime(english_date, '%d %B %Y')
        return  python_date.strftime('%Y-%m-%d')

    def process_item(self, item, spider):
        match = re.search('\d+', item['altitude'])
        if match:
            item['altitude'] = match.group(0)
        else:
            item['altitude'] = ''


        match = re.search('\d+', item['denivele'])
        if match:
            item['denivele'] = match.group(0)
        else:
            item['denivele'] = ''


        if 'dep_url' in item.keys():
            match = re.search('\d+', item['dep_altitude'])
            if match:
                item['dep_altitude'] = match.group(0)
            else:
                item['dep_altitude'] = None

            match = re.search('(\d+.\d+) N / (\d+.\d+) E',   item['dep_latlon'])
            if match:
                lat = float(match.group(1))
                lon = float(match.group(2))
                item['dep_geojson'] = Point((lon, lat, int(item['dep_altitude'])))
            else:
                item['dep_geojson'] = None

        date = re.search('\w+ (\d{2} \w+ \d{4})', item['date']).group(1)
        item['date'] = self.convert_date(date)

        snow_quality = len(item['snow_quality'].strip())
        item['snow_quality'] = snow_quality
        return item


class TripWriterPipeline(object):
    collection_name = 'trip'
    trip_field = ["nom" , "trip_url"  , "orientation",
                 "dep_gps" , "altitude" , "massif",
                 "secteur" , "dep_name" , "dep_altitude",
                 "pente" , "dep_url" , "dep_geojson",
                 "diff_ski" , "diff_monte" , "nb_jours" ,
                 "dep_latlon" , "denivele", "trip_id"]

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE', 'items')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        item_dict = {}
        for keys in self.trip_field:
            if keys in item.keys():
                item_dict[keys] = item[keys]

        trip_id = item['trip_id']
        query_id = {'trip_id': trip_id}
        trip = self.db[self.collection_name].find(query_id)

        if trip.count() > 0:
            logging.log(logging.INFO,'duplicate trip found')
            return item
        else:
            self.db[self.collection_name].insert(item_dict)
            return item

class SnowQuallityWriterPipeline(object):
    collection_name = 'condition'
    trip_field = ["snow_quality_id", "nom",
                  "date", "snow_quality"]

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE', 'items')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        item_dict = {}
        for keys in self.trip_field:
            if keys in item.keys():
                item_dict[keys] = item[keys]

        snow_quality_id = item['snow_quality_id']
        query_id = {'snow_quality_id': snow_quality_id}
        condition = self.db[self.collection_name].find(query_id)
        print self.db

        if condition.count() > 0:
            logging.log(logging.INFO,'duplicate conditions found')
            return item
        else:
            self.db[self.collection_name].insert(item_dict)
            return item
