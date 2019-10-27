# -*- coding: utf-8 -*-
"""
功能：本程序从东方财富网提取盈利预测的最新变化情况，保存sqlite
用法：每天运行
"""
import time
import datetime
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
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    cn = sqlite3.connect(dbfn)
    '''
    股票代码,日期(抓取日期)，研报数,机构投资评级（近6个月）数A买入、B增持、C中性、D减持、E卖出
    上年实际EPS，当年预测EPS，后一年预测EPS，后二年预测EPS
    注意：5月1日前
    '''
    cn.execute('''CREATE TABLE IF NOT EXISTS YLYC
           (GPDM TEXT NOT NULL,
           RQ TEXT NOT NULL,
           YBS INT,
           PJA INT,
           PJB INT,
           PJC INT,
           PJD INT,
           PJE INT,
           NF_1 TEXT,
           EPS_1 REAL,
           NF TEXT,
           EPS REAL,
           NF1 TEXT,
           EPS1 REAL,
           NF2 TEXT,
           EPS2 REAL
           );''')
    cn.execute('''CREATE UNIQUE INDEX IF NOT EXISTS YLYC_RQ_GDHS ON YLYC(GPDM,RQ);''')



def getylyc(pgn):
    
    print("正在处理第%d页，请等待。" % pgn)
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)

    browser = webdriver.PhantomJS()
#    browser = webdriver.Firefox()
    browser.get("http://data.eastmoney.com/report/ylyc.html")
    time.sleep(5)

    try :    
        #定位到表头
        thead = browser.find_elements_by_tag_name("thead")
        #表头行数
        thdrows = thead[0].find_elements_by_tag_name('tr')
        thdcols = thdrows[0].find_elements_by_tag_name('th')
        nf_1 = thdcols[8].text
        nf = thdcols[9].text
        nf1 = thdcols[10].text
        nf2 = thdcols[11].text
        
        elem = browser.find_element_by_id("gopage")
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
        
    except :
        dbcn.close()
        print('1异常退出')
        browser.quit()
        
        return False

    #遍历行
    data = []
    for j in range(len(tblrows)):
        
        try :    
            tblcols = tblrows[j].find_elements_by_tag_name('td')
            '''
            股票代码,日期(抓取日期)，研报数,机构投资评级（近6个月）数A买入、B增持、C中性、D减持、E卖出
            上年实际EPS，当年预测EPS，后一年预测EPS，后二年预测EPS
            注意：5月1日前
            '''
            
            dm = tblcols[1].text
            ybs = tblcols[6].text
            pja = tblcols[7].text
            pjb = tblcols[8].text
            pjc = tblcols[9].text
            pjd = tblcols[10].text
            pje = tblcols[11].text
            eps_1 = tblcols[12].text
            
            eps = tblcols[13].text
            eps1 = tblcols[15].text
            eps2 = tblcols[17].text
                       
                       
        except :
            dbcn.close()
            print('2异常退出')
            browser.quit()
            return False

        if dm[0] in ('0','3','6'):
            dm = lgpdm(dm)       
            rq = datetime.datetime.now().strftime('%Y%m%d')
            rowdat = [dm,rq,ybs,pja,pjb,pjc,pjd,pje,nf_1,eps_1,nf,eps,nf1,eps1,nf2,eps2]
            rowdat = [e if e!='-' else np.nan for e in rowdat]
  
            data.append(rowdat)
    
    if len(data)>0 :
        dbcn.executemany('''INSERT OR REPLACE INTO YLYC (GPDM,RQ,YBS,PJA,
                            PJB,PJC,PJD,PJE,NF_1,EPS_1,NF,EPS,NF1,EPS1,NF2,EPS2) 
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', data)

        dbcn.commit()

    dbcn.close()
    browser.quit()

    return True

def getdrive():
    return sys.argv[0][:2]

def main():
    createDataBase()
    j=1
    while j<=71:
        if getylyc(j):
            j+=1


if __name__ == "__main__": 
    now1 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now1)

    main()
    
    now2 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now1)
    print('结束运行时间：%s' % now2)
    
    
