import subprocess
import schedule
import time
import os
import json

current_dir = os.getcwd()

with open('config.json', 'r') as f:
    config = json.load(f)

def launch_skitour_crawl(spider_dir, spider_name):
    os.chdir(spider_dir)
    os.system("scrapy crawl {0}".format(spider_name))

def launch_meteofrance_crawl(meteofrance_dir):
    os.chdir(meteofrance_dir)
    os.system("python import_snow_data.py")

def daily_job():
    os.chdir(current_dir)
    launch_skitour_crawl(config['spider_dir'], config['spider_name'])
    os.chdir(current_dir)
    launch_meteofrance_crawl(config['meteofrance_dir'])

schedule.every().day.at("07:45").do(daily_job)
#schedule.every(1).minutes.do(daily_job)

while True:
    schedule.run_pending()
    time.sleep(1)
