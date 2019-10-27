# -*- coding: utf-8 -*-
"""
功能：本程序从东方财富网提取股票回购数据，保存sqlite
用法：每天运行
发现这个汇总列表不全，与

和讯网 上市公司2017年年度业绩快报
http://datainfo.hexun.com/wholemarket/html/yjkb.aspx?data_type=fld_released_date&page=51
lbdq
"""
import time
from selenium import webdriver
import sqlite3
import sys
import re
import numpy as np

###############################################################################
#万亿转换
###############################################################################
def wyzh(str):
    wy=re.findall('(.+)亿',str)
    if len(wy)==1 :
        return float(wy[0])*100000000
    wy=re.findall('(.+)万',str)
    if len(wy)==1 :
        return float(wy[0])*10000

    return 0

###############################################################################
#长股票代码
###############################################################################
def lgpdm(dm):
    return dm[:6]+('.SH' if dm[0]=='6' else '.SZ')

###############################################################################
#短股票代码
###############################################################################
def sgpdm(dm):
    return dm[:6]

########################################################################
#建立数据库业绩快报
########################################################################
def createDataBase():
    dbfn=getdrive()+'\\hyb\\STOCKDATA.db'
    cn = sqlite3.connect(dbfn)
    '''
    股票代码,业绩预约披露日期
														
    '''
    cn.execute('''CREATE TABLE [GPHG](
                  [GPDM] TEXT NOT NULL, 
                  [GGRQ] TEXT NOT NULL, 
                  [SSJD] TEXT NOT NULL, 
                  [QSSJ] TEXT);
           ''')

    cn.execute('''CREATE UNIQUE INDEX [GPDM_GGRQ_GPHG]
                    ON [GPHG](
                      [GPDM], 
                      [GGRQ]);

           ''')
                   
'''    
CREATE TABLE [GPHG](
  [GPDM] TEXT NOT NULL, 
  [GGRQ] TEXT NOT NULL, 
  [SSJD] TEXT NOT NULL, 
  [QSSJ] TEXT);

CREATE UNIQUE INDEX [GPDM_GGRQ_GPHG]
ON [GPHG](
  [GPDM], 
  [GGRQ]);

'''


def getdrive():
    return sys.argv[0][:2]


if __name__ == "__main__": 
    print('%s Running' % sys.argv[0])
    dbfn=getdrive()+'\\hyb\\STOCKDATA.db'
    dbcn = sqlite3.connect(dbfn)

    url='http://data.eastmoney.com/gphg/'
    browser = webdriver.PhantomJS()
    browser.get(url)
    time.sleep(5)
    try:
        elem = browser.find_element_by_id("PageCont")
        pgs=int(elem.find_element_by_xpath("//a[@title='转到最后一页']").text)
    except:
        browser.quit()
        exit()
        
    
    pgn=1
    while pgn<=pgs:

        print("正在处理第%d/%d页，请等待。" % (pgn,pgs))
        elem = browser.find_element_by_id("PageContgopage")
        elem.clear()
        #输入页面
        elem.send_keys(pgn)
        elem = browser.find_element_by_class_name("btn_link")     
        #点击Go
        elem.click()
        time.sleep(5)
        #定位到表体
        tbody = browser.find_elements_by_tag_name("tbody")
        #表体行数
        tblrows = tbody[0].find_elements_by_tag_name('tr')


        #遍历行
        data = []
        sc=True     #本页处理成功
        for j in range(len(tblrows)):
            try:
            
                tblcols = tblrows[j].find_elements_by_tag_name('td')
                
                dm = tblcols[1].text
                ggrq = tblcols[13].text
                ssjd = tblcols[12].text                
                qssj = tblcols[11].text
                

                if dm[0] in ('0','3','6'):
        
                    dm = lgpdm(dm)       
                    data.append([dm,ggrq,ssjd,qssj])
            except:
                sc=False    #本页处理不成功
                break
        
        if len(data)>0 and sc:
            dbcn.executemany('''INSERT OR REPLACE INTO GPHG (GPDM,GGRQ,SSJD,QSSJ) 
                                VALUES (?,?,?,?)''', data)
            dbcn.commit()
            pgn+=1
        else:
            browser.get(url)
            time.sleep(5)
            

    dbcn.close()

    browser.quit()
