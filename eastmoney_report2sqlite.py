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

def jgdic():
    return {
        '高盛':'高盛高华',
        '国泰君安国际':'国泰君安',
        '群益证券(香港)':'群益证券',
        '申万宏源研究':'申万宏源',
        '新时代证券':'新时代',
        '银河国际':'银河证券',
        '银河国际(香港)':'银河证券',
        '元大证券(香港)':'元大证券',
        '元大证券股份有限公司':'元大证券',
        '中国银河':'银河证券',
        '中国银河国际':'银河证券',
        '中国银河国际证券':'银河证券',
        '中信建投(国际)':'中信建投',
        '中信建投证券':'中信建投'
        }

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
    股票代码,日期(发布日期)，研报标题,评级，评级变动，机构名称，年份一，年份一EPS，年份二，年份二EPS   注意：5月1日前
    '''
    cn.execute('''CREATE TABLE IF NOT EXISTS REPORT
           (GPDM TEXT NOT NULL,
           RQ TEXT NOT NULL,
           YBBT TEXT,
           WZ TEXT,
           PJ TEXT,
           PJBD TEXT,
           PJJG TEXT NOT NULL,
           NF1 TEXT,
           EPS1 REAL,
           NF2 TEXT,
           EPS2 REAL
           );''')
    cn.execute('''CREATE UNIQUE INDEX IF NOT EXISTS GPDM_RQ_PJJG ON REPORT(GPDM,RQ,PJJG);''')



def getrpt(pgn):
    pjjgdic=jgdic()
    
    print("正在处理第%d页，请等待。" % pgn)
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)

    browser = webdriver.PhantomJS()
#    browser = webdriver.Firefox()
    browser.get("http://data.eastmoney.com/report/")
    time.sleep(5)

    try :    
        #定位到表头
        thead = browser.find_elements_by_tag_name("thead")
        #表头行数
        thdrows = thead[0].find_elements_by_tag_name('tr')
        thdcols = thdrows[0].find_elements_by_tag_name('th')
        nf1 = thdcols[9].text
        nf2 = thdcols[10].text
        
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
            dm = tblcols[2].text
            rq = tblcols[1].find_elements_by_tag_name('span')[0].get_property("title")         
            bt = tblcols[5].find_elements_by_tag_name('a')[0].text         
            wz = tblcols[5].find_elements_by_tag_name('a')[0].get_attribute('href')         

            pj = tblcols[6].text
            pjbd = tblcols[7].text
            pjjg = tblcols[8].find_elements_by_tag_name('a')[0].text
            
            if pjjg in pjjgdic.keys():
                pjjg=pjjgdic[pjjg]
            
            eps1 = tblcols[9].text
            eps2 = tblcols[11].text
                       
                       
        except :
            dbcn.close()
            print('2异常退出')
            browser.quit()
            return False

        if dm[0] in ('0','3','6'):
            dm = lgpdm(dm)       
#            rq = datetime.datetime.now().strftime('%Y%m%d')
            rowdat = [dm,rq,bt,wz,pj,pjbd,pjjg,nf1,eps1,nf2,eps2]
            rowdat = [e if e!='-' else np.nan for e in rowdat]
  
            data.append(rowdat)
    
    if len(data)>0 :
        dbcn.executemany('''INSERT OR REPLACE INTO REPORT (GPDM,RQ,YBBT,WZ,PJ,
                            PJBD,PJJG,NF1,EPS1,NF2,EPS2) 
                            VALUES (?,?,?,?,?,?,?,?,?,?,?)''', data)

        dbcn.commit()

    dbcn.close()
    browser.quit()

    return True

def getdrive():
    return sys.argv[0][:2]

def main():
    createDataBase()
    j=1
    while j<=15:
        if getrpt(j):
            j+=1


if __name__ == "__main__": 
    print('%s Running' % sys.argv[0])

    now1 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now1)

    main()
    
    now2 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now1)
    print('结束运行时间：%s' % now2)
    
    
