# -*- coding: utf-8 -*-
"""
Created on Fri Dec 21 18:53:35 2018

@author: lenovo
"""

import os
import sys
import re
import datetime
from configobj import ConfigObj
import sqlite3
import numpy as np
import pandas as pd
import xlwings as xw
import struct
import winreg
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pyquery import PyQuery as pq
import time
import tushare as ts
from urllib import request
import zipfile
from bs4 import BeautifulSoup as bs
import dateutil.parser
import xlrd
import subprocess

##########################################################################
#获取运行程序所在驱动器
##########################################################################
def getdrive():
    if sys.argv[0]=='' :
        return os.path.splitdrive(os.getcwd())[0]
    else:
        return os.path.splitdrive(sys.argv[0])[0]


if __name__ == '__main__':
    
#    sys.exit()
    
    recent_days=5
    today=datetime.datetime.now().strftime("%Y%m%d")
    
    #tushare 通过Python SDK 调取数据
    #https://tushare.pro/document/1?doc_id=131
    
    mytoken='18fcea168f6c1f8621c13bef376e726cf5e31fde3f579db37929181b'
    pro = ts.pro_api(token=mytoken)

    calfn=r'd:\selestock\cal.csv'
    if not os.path.exists(calfn):
        cal=pro.trade_cal(start_date='19901219',end_date='20180505',fields='cal_date,is_open,pretrade_date')
        cal=cal.append(pro.trade_cal(start_date='20180506',end_date='20191231',fields='cal_date,is_open,pretrade_date'))
    
        cal=cal.sort_values(by='cal_date',ascending=False)
        cal=cal[(cal['is_open']==1)]
        cal.to_csv(calfn,index=False)
    else:
        cal= pd.read_csv(calfn, dtype={'cal_date':'object','pretrade_date':'object'})
        
    cal=cal[(cal['cal_date']<=today)]
    
    cal.index=cal['cal_date'].rank(ascending=False)
    
    cal.index.name='num'
    
    for i in range(1,recent_days):
        rq=cal.loc[i]['cal_date']
        print(rq)

        adjdf = pro.adj_factor(trade_date=rq)
        
        csvfn=getdrive()+'\\tdx\\adj\\%s.csv' % rq

        adjdf.to_csv(csvfn,index=False)
        

