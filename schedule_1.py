# -*- coding: utf-8 -*-
"""
Created on Sat Feb  2 22:02:03 2019

@author: lenovo
"""

import schedule
import time

def job():
    print("I'm working...")
    
schedule.every(10).seconds.do(job)#每隔10秒执行函数job
schedule.every(10).minutes.do(job)#每隔10分钟执行函数job
schedule.every().hour.do(job)#每隔1小时执行函数job
schedule.every().day.at("10:30").do(job) #每天的10点半执行函数job 
schedule.every().monday.do(job)#每周一执行函数job
schedule.every().wednesday.at("13:15").do(job)  #每周三下午1点14分执行函数job
while True:
    schedule.run_pending() #执行任务
    time.sleep(1)