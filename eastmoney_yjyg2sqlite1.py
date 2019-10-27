# -*- coding: utf-8 -*-
"""
功能：本程序从东方财富网提取业绩预告情况，保存sqlite
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
    股票代码,日期，业绩变动,业绩变动范围,业绩变动幅度,预告类型,上年同期净利润
    '''
    cn.execute('''CREATE TABLE IF NOT EXISTS YJYG
           (GPDM TEXT NOT NULL,
           GPMC TEXT,
           RQ TEXT NOT NULL,
           YJBD TEXT,
           YJBDFW TEXT,
           YJBDFD1 REAL,
           YJBDFD2 REAL,
           YJBDFD REAL,
           YGLX TEXT,
           JLR_1 REAL
           );''')
    cn.execute('''CREATE UNIQUE INDEX IF NOT EXISTS YJYG_RQ_GDHS ON YJYG(GPDM,RQ);''')



def getyjyg(bgq,pgn):
    nf=bgq[:4]
    jd=bgq[4:]
    jddic={'12':'12.31','03':'03.31','06':'06.30','12':'12.31'}
    print("正在处理第%d页，请等待。" % pgn)
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)

    browser = webdriver.PhantomJS()
#    browser = webdriver.Firefox()
    url = "http://data.eastmoney.com/bbsj/%s/yjyg.html" % bgq
    browser.get(url)
    time.sleep(5)

    try :    
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
            股票代码,日期，业绩变动,业绩变动范围,业绩变动幅度,预告类型,上年同期净利润
            '''
            
            dm = tblcols[1].text
            mc = tblcols[2].text
            yjbd = tblcols[4].text
            
            yjbdfw = tblcols[6].text
            
            yjbdfd = '-'
            yjbdfd1 = '-'
            yjbdfd2 = '-'
            
            if yjbdfw != '-' :
                yjbdfx=re.findall('(-?\d*\.?\d+)\%',yjbdfw)
                if len(yjbdfx)==2 :
                    yjbdfd1=float(yjbdfx[0])
                    yjbdfd2=float(yjbdfx[1])
                    yjbdfd = round((yjbdfd1+yjbdfd2)/2,2)
                if len(yjbdfx)==1 :
                    yjbdfd1 = float(yjbdfx[0])
                    yjbdfd2 = float(yjbdfx[0])
                    yjbdfd = float(yjbdfx[0])
                
            yglx = tblcols[8].text
            jlr_1 = tblcols[9].text
            if jlr_1!='-' :
                jlr_1 = wyzh(jlr_1)
                       
        except :
            dbcn.close()
            print('2异常退出')
            browser.quit()
            return False

        if dm[0] in ('0','3','6') :
            dm = lgpdm(dm)       
            rq = '%s.%s' % (nf,jddic[jd])
            rowdat = [dm,mc,rq,yjbd,yjbdfw,yjbdfd1,yjbdfd2,yjbdfd,yglx,jlr_1]
            rowdat = [e if e!='-' else np.nan for e in rowdat]
            data.append(rowdat)
    
    if len(data)>0 :
        dbcn.executemany('''INSERT OR REPLACE INTO YJYG (GPDM,GPMC,RQ,YJBD,YJBDFW,
                            YJBDFD1,YJBDFD2,YJBDFD,YGLX,JLR_1) 
                            VALUES (?,?,?,?,?,?,?,?,?,?)''', data)

        dbcn.commit()

    dbcn.close()
    browser.quit()

    return True

def getdrive():
    return sys.argv[0][:2]

def main():
    createDataBase()
    bgq='201806'
    j=1
    while j<=46:
        if getyjyg(bgq,j):
            j+=1


if __name__ == "__main__": 
    now1 = datetime.datetime.now().strftime('%H:%M:%S')
    print('%s Running' % sys.argv[0])
    main()

    now2 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now1)
    print('结束运行时间：%s' % now2)
