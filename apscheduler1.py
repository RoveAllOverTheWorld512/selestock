# -*- coding: utf-8 -*-
"""
Created on Sun Feb  3 10:09:23 2019

@author: lenovo
"""

from apscheduler.schedulers.blocking import BlockingScheduler
import datetime
import time
def tick():
    print('Tick! The time is: %s' % datetime.datetime.now())
    while True:
        time.sleep(3)
        print("11111")
        
def run():
    #调用了 APScheduler 模块
    scheduler = BlockingScheduler()
    scheduler.add_job(tick,'interval',seconds=2) #tick也可以传参数，3秒执行tick函数
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
run()