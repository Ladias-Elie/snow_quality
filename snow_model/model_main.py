import subprocess
import schedule
import time
import os
import json

current_dir = os.getcwd()

with open('config.json', 'r') as f:
    config = json.load(f)

def daily_job():
    os.chdir(current_dir)
    os.system("python predict_condition.py")

schedule.every().day.at("08:00").do(daily_job)

while True:
    schedule.run_pending()
    time.sleep(1)
