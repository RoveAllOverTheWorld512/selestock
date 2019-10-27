# -*- coding: utf-8 -*-
"""
Created on Tue Sep 11 19:04:35 2018

@author: lenovo
"""

from selenium import webdriver
import time
import re
import pandas as pd
from pyquery import PyQuery as pq
import dateutil.parser

fn=r'd:\selestock\hxy.xlsx'
writer=pd.ExcelWriter(fn,engine='xlsxwriter')

browser = webdriver.Firefox()

for y in range(2013,2019):
    url='https://www.boxofficemojo.com/yearly/chart/?yr=%d&view=releasedate&view2=domestic&sort=gross&order=DESC&&p=.htm' % y
    
    browser.get(url)

    time.sleep(3)
    
    xp='/html/body/div[1]/div[3]/div[2]/table[3]/tbody/tr/td[1]/table/tbody/tr[2]/td/table/tbody'
    
    tbl=browser.find_element_by_xpath(xp).get_attribute('innerHTML')
    
    rows=pq(tbl)('tr')
    
    data=[]
    
    for i in range(2,17):
        cols=pq(rows.eq(i))('td')
        Rank=cols.eq(0).text()
        Title=cols.eq(1).text()
        Studio=cols.eq(2).text()
        Total_Gross=cols.eq(3).text().replace('$','').replace(',','')
        Theaters1=cols.eq(4).text().replace('$','').replace(',','')
        Opening=cols.eq(5).text().replace('$','').replace(',','')
        Theaters2=cols.eq(6).text().replace('$','').replace(',','')
        Open=re.findall('date=(.*)&amp',cols.eq(7).html())[0]
        Open=dateutil.parser.parse(Open).strftime("%Y-%m-%d")
        close=cols.eq(8).text()
        if close!='-':
            Close=str(y)+'-'+cols.eq(8).text().replace('/','-')
            
            Close=dateutil.parser.parse(Close).strftime("%Y-%m-%d")
            if Close<Open :
                Close=str(y+1)+'-'+cols.eq(8).text().replace('/','-')
                Close=dateutil.parser.parse(Close).strftime("%Y-%m-%d")
        else:
            Close=None
            
        dat=[Rank,Title,Studio,Total_Gross,Theaters1,Opening,Theaters2,Open,Close]    
    
        data.append(dat)
        
        
    cols=['Rank','Title','Studio','Total_Gross','Theaters1','Opening','Theaters2','Open','Close']    
    
    df=pd.DataFrame(data,columns=cols)

    df.to_excel(writer, sheet_name=str(y),index=False)   

writer.save()

browser.quit()