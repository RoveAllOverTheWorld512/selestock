# -*- coding: utf-8 -*-
"""
Created on Sun Dec  9 09:22:54 2018

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
#股票列表
##########################################################################
def get_stklst():
    
#    mytoken='18fcea168f6c1f8621c13bef376e726cf5e31fde3f579db37929181b'
#    pro = ts.pro_api(token=mytoken)
    #df = pro.daily(trade_date='20181206')
    
    #data = pro.stock_basic(exchange='', list_status='D', fields='ts_code,symbol,name,area,industry,list_date')
    
    dt0 = pro.stock_basic(list_status='L',fields='ts_code,symbol,name,area,industry,list_date')
    dt0=dt0.set_index('ts_code',drop=False)
    
    dt1 = pro.stock_basic(list_status='P',fields='ts_code,symbol,name,area,industry,list_date')
    dt1=dt1.set_index('ts_code',drop=False)
    
    dt2 = pro.stock_basic(list_status='D',fields='ts_code,symbol,name,area,industry,list_date')
    dt2=dt2.set_index('ts_code',drop=False)
    
    dt=pd.concat([dt0,dt1,dt2])
        
    dt=dt[~dt.index.duplicated()]

    return dt[['ts_code','name']]

def get_stknm(gpdm):
        
#    mytoken='18fcea168f6c1f8621c13bef376e726cf5e31fde3f579db37929181b'
#    pro = ts.pro_api(token=mytoken)
    return pro.namechange(ts_code=gpdm, fields='ts_code,name,start_date,end_date,change_reason')

if __name__ == '__main__':
    mytoken='18fcea168f6c1f8621c13bef376e726cf5e31fde3f579db37929181b'
    pro = ts.pro_api(token=mytoken)
    gpdmb=get_stklst()
    dt=get_stknm(gpdmb.iloc[0].ts_code)
    for i in range(1,len(gpdmb)):
        gpdm=gpdmb.iloc[i].ts_code
        
        print(gpdm)
        df=get_stknm(gpdm)
        dt=dt.append(df)
    
#    #提取股票名称包含ST
#    dt1=dt[dt['name'].str.contains('ST')]
#
#    dt1=dt1.sort_values(by='start_date',ascending=False)
#    #股票代码去重    
#    dt2=dt1[~dt1['ts_code'].duplicated()]
#    
#    dt0 = pro.stock_basic(list_status='L',fields='ts_code,symbol,name,area,industry,list_date')
#    dt0=dt0.set_index('ts_code',drop=False)
#    
#    #正在上市
#    dt3=dt2[dt2['ts_code'].isin(dt0.index)]
#
#    #已经退市
#    dt4=dt2[~dt2['ts_code'].isin(dt0.index)]
#    
#    dt5=dt[dt['ts_code'].isin(dt0.index)]
#    dt5=dt5.sort_values(by='start_date',ascending=False) 
#    dt5=dt5.set_index('ts_code',drop=False)
#    dt5=dt5[~dt5.index.duplicated(keep='first')]
#    dt5=dt5[dt5.index.isin(dt3['ts_code'])]
#    
#    fn = r'd:\selestock\ST股票汇总.xlsx'
#
#    if os.path.exists(fn):
#        os.remove(fn)
#
#    writer=pd.ExcelWriter(fn,engine='xlsxwriter')
#
#    dt3.to_excel(writer, sheet_name='正在上市',index=False)   
#    dt5.to_excel(writer, sheet_name='当前名称',index=False)  
#    dt4.to_excel(writer, sheet_name='已经退市',index=False)  
#
#    writer.save()
