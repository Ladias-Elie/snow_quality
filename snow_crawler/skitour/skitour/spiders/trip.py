# -*- coding: utf-8 -*-
import scrapy
import unicodedata
import hashlib
import logging


from skitour.items import SkitourItem


class TripSpider(scrapy.Spider):
    name = "trip_skitour"
    allowed_domains = ["skitour.fr"]
    #start_urls = (
    #    'http://www.skitour.fr/topos/dernieres-sorties.php',
    #)

    def start_requests(self):
        "Overwrite start_request scrapy method"
        start_url = 'http://www.skitour.fr/topos/dernieres-sorties.php'
        for p in range(0,5):
            url = start_url+'?p={0}'.format(p)
            yield scrapy.Request(url, self.parse)



    def parse(self, response):
        for href in response.css("td a::attr('href')"):
            url = response.urljoin(href.extract())
            yield scrapy.Request(url, callback = self.parse_trip)

    def parse_trip(self, response):
        main_info_key = response.xpath("//table/tr/td/strong/text()").extract()
        clean_key = [k.replace('[', '') for k in main_info_key if k != u'] : ']
        normalize_key_list = [unicodedata.normalize('NFKD', x) for x in clean_key]
        decoded_key_list = [x.encode('ascii', 'ignore').decode('UTF-8') for x in normalize_key_list]
        key_list = [x.replace(':', '') for x in decoded_key_list]

        main_info_value = response.xpath("//table/tr/td/text()").extract()
        clean_value = [x for x in main_info_value if '\n' not in x and '\t' not in x and '\r' not in x]
        normalize_value_list = [unicodedata.normalize('NFKD', x) for x in clean_value]
        decoded_value_list = [x.encode('ascii', 'ignore').decode('UTF-8') for x in normalize_value_list]
        value_list = [x.replace(':', '') for x in decoded_value_list]



        item = SkitourItem()
        n = len(key_list) - 3
        main_info_dic = dict(zip(key_list[0:n], value_list[0:n]))

        main_info = response.xpath('//h1//text()').extract()
        item['trip_url'] = unicode(response.url, 'UTF-8')
        item['nom'] = main_info[1]
        item['altitude'] = main_info[0]
        item['massif'] = main_info_dic['Massif  ']

        try:
            secteur = main_info_dic['Secteur  ']
        except KeyError:
            logging.log(logging.INFO,
            "No key Secteur for this page: {0}".format(response.url))
            secteur = u''
        item['secteur'] = secteur
        item['orientation'] = main_info_dic['Orientation  ']
        item['denivele'] = main_info_dic['Denivele  ']
        item['diff_monte'] = main_info_dic['Difficulte de montee ']
        item['diff_ski'] = main_info_dic['Difficulte ski ']
        item['pente'] = main_info_dic['Pente  ']
        item['nb_jours'] = main_info_dic['Nb de jours  ']

        #get some snow quality info data
        snow_quality = response.xpath("//*[(@id = 'sortie')]//p/text()")
        item['snow_quality'] = snow_quality.extract()[1][3::]
        date_info = response.xpath("//div[(@id = 'sortie')]//h2/text()").extract()[1]
        item['date'] = date_info

        #create ids for the two databases
        trip_id_string = item['nom'].replace(' ','').encode('UTF-8')
        item['trip_id'] = hashlib.md5(trip_id_string).hexdigest()

        snow_quality_id_string = (item['nom'] + item['date']).replace(' ','').encode('UTF-8')
        item['snow_quality_id'] = hashlib.md5(snow_quality_id_string).hexdigest()

        try:
            start_url = response.css("td+ td strong+ a::attr('href')")[0].extract()
        except IndexError:
            return item

        start_page = response.urljoin(start_url)
        request = scrapy.Request(start_page,
                                 callback=self.parse_start)
        request.meta['item'] = item
        return request

    def parse_start(self, response):
        item = response.meta['item']
        start_infos = response.xpath('//li//text()').extract()
        normalize_start_infos = [unicodedata.normalize('NFKD', x) for x in start_infos]
        decode_start_infos = [x.encode('ascii', 'ignore').decode('UTF-8') for x in normalize_start_infos]
        clean_start_infos = [x.replace(':', '') for x in decode_start_infos]

        start_dic = dict(zip(clean_start_infos[::2], clean_start_infos[1::2]))

        item['dep_url'] = unicode(response.url, 'UTF-8')
        item['dep_name'] = response.xpath('//h1//text()').extract()[0]
        item['dep_altitude'] = start_dic['Altitude ']
        item['dep_latlon'] = start_dic['Lat/Lon']
        item['dep_gps'] = start_dic['GPS']
        return item
