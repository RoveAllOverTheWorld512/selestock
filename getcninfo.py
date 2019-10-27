# -*- coding: utf-8 -*-
"""
Created on Tue May 21 17:20:39 2019

@author: lenovo

python中的urlencode与urldecode
https://www.cnblogs.com/caicaihong/p/5687522.html

"""
import dateutil.parser
import datetime
import time
import urllib.parse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sqlite3
import sys
import os
import re
import pandas as pd
from pyquery import PyQuery as pq
import selestock as my

def get_info_list():
    fireFoxOptions = webdriver.FirefoxOptions()
    fireFoxOptions.headless=True
    browser = webdriver.Firefox(options=fireFoxOptions)

#    browser = webdriver.Firefox()

    s='问询函'
    keyword=urllib.parse.quote(s)
    url='http://www.cninfo.com.cn/new/fulltextSearch?keyWord=%s' % keyword

    browser.get(url)
        
    rangeA='2019-01-01 ~ 2019-05-22'

    elem=WebDriverWait(browser, 20).until(
        EC.presence_of_element_located((By.XPATH, "//input[@id='rangeA']")))
    
    elem.clear()

    elem.send_keys(rangeA)
    
    elem = browser.find_element_by_class_name("cnfont-search")     
    #点击Go
    elem.click()

    
    elem=WebDriverWait(browser, 20).until(
        EC.presence_of_element_located((By.XPATH, "//a[text()='时间排序']")))
    
    elem.click()
    
#    "//标签名[contains(@属性, '属性值')]"
    elem = browser.find_element_by_xpath("//span[contains(text(),'合计约')]")
    n=re.findall('\d+',elem.text)
    if len(n)==1:
        n=int(n[0])
    
    m=int((n+9)/10)

#    data=[]
    for i in range(m):

        print(i,m)

#        elem=WebDriverWait(browser, 20).until(
#                EC.presence_of_element_located((By.XPATH, "//table[@id='ul_a_title']")))

        #最多读取5次,确保网页数据调取完成后读取网页内容，在数据没有调取完成前
        #html为空，无法进行后续处理
        for j in range(5):
            elem = browser.find_element_by_id("ul_a_title")
            html = elem.get_attribute("innerHTML")         
            if len(html)>0:
                break
            else:
                time.sleep(3)
                continue

        #html不能为空    
        if len(html)>0:
            tbl=pq(html)
            trs=tbl('tr')
            for j in range(len(trs)):
                row=pq(trs.eq(j))
                tds=row('td')
                a=tds.eq(0).html()
                b=tds.eq(0).text()
                c=tds.eq(1).text()
                
                #有的包含时间，这里只取日期
                c=dateutil.parser.parse(c).strftime('%Y-%m-%d')
                
                d= re.findall('stockCode=(\d+)',a)[0]
                   
                if len(d)==6:
#                    if '600998' in d:
#                        print(b)
#                        print((not '半年' in b))
#                        print((('年度' in b) or ('年报' in b)))
#                        print((not '半年' in b) and (('年度' in b) or ('年报' in b)))

                    #去掉半年报
                    if (d in agdmlst) and (not '半年' in b) and (('年度' in b) or ('年报' in b)) :
                        d=my.lgpdm(d)
                        data.append([d,b,c])
                
            
        elem = browser.find_element_by_xpath("//a[text()='>']")
        elem.click()

        time.sleep(3)
        
    browser.quit()
    
#    df=pd.DataFrame(data,columns=['gpdm','ggmc','date'])
#    
#    df.to_csv(r'd:\selestock\nbwxh.csv',encoding='GBK',index=False)

    return


if __name__ == "__main__": 
    
    gpdmb=my.get_gpdm()
    
    #A股代码列表
    agdmlst=gpdmb['dm'].tolist()

    data=[]
    get_info_list()

    df=pd.DataFrame(data,columns=['gpdm','ggmc','date'])

    df=df.set_index('gpdm',drop=True)
   
    df=df.join(gpdmb)
    
    #选取有用列
    cols='gpdm,gpmc,gppy,ggmc,date'.split(',')
    df=df[cols]
    
    #按日期逆序排列
    df=df.sort_value(by='date',ascending=False)
    
    #保存
    df.to_csv(r'd:\selestock\nbwxh.csv',encoding='GBK',index=False)
    