# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html
import scrapy

class SkitourItem(scrapy.Item):
    trip_url = scrapy.Field()
    nom = scrapy.Field()
    altitude = scrapy.Field()
    massif = scrapy.Field()
    secteur = scrapy.Field()
    orientation = scrapy.Field()
    denivele = scrapy.Field()
    diff_monte = scrapy.Field()
    diff_ski = scrapy.Field()
    pente = scrapy.Field()
    nb_jours = scrapy.Field()
    type_rando = scrapy.Field()
    dep_url = scrapy.Field()
    dep_name = scrapy.Field()
    dep_altitude = scrapy.Field()
    dep_latlon = scrapy.Field()
    dep_gps = scrapy.Field()
    dep_geojson = scrapy.Field()
    snow_quality = scrapy.Field()
    date = scrapy.Field()
    trip_id = scrapy.Field()
    snow_quality_id = scrapy.Field()
