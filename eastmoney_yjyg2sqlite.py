# -*- coding: utf-8 -*-
"""
功能：本程序从东方财富网提取业绩预告情况，保存sqlite
用法：每天运行
"""
import time
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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



def getdrive():
    return sys.argv[0][:2]



if __name__ == "__main__": 
    now1 = datetime.datetime.now().strftime('%H:%M:%S')
    print('%s Running' % sys.argv[0])

    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)

    rq = '2018.06.30'
    bgq=rq[:4]+rq[5:7]

    url = "http://data.eastmoney.com/bbsj/%s/yjyg.html" % bgq
    
#    fireFoxOptions = webdriver.FirefoxOptions()
#    fireFoxOptions.set_headless()
#    browser = webdriver.Firefox(firefox_options=fireFoxOptions)

    chrome_options = webdriver.ChromeOptions() 
    chrome_options.add_argument("--headless") 
    chrome_options.add_argument('--disable-gpu')
    browser = webdriver.Chrome(chrome_options=chrome_options) 
    
    browser.get(url)

    try:
        '''
        EC.presence_of_element_located()传递的参数是tuple元组
        '''
        elem=WebDriverWait(browser, 3).until(
            EC.presence_of_element_located((By.XPATH, "//a[text()='下一页']//preceding-sibling::a[1]")))
        pgs=int(elem.text)
    except:
        pgs=1


    pgn=1   
#    pgs=1
    while pgn<=pgs:

        print("正在处理第%d/%d页，请等待。" % (pgn,pgs))
        if pgn>1:
            try :    
                elem = browser.find_element_by_id("PageContgopage")
                elem.clear()
                #输入页面
                elem.send_keys(pgn)
                elem = browser.find_element_by_class_name("btn_link")     
                #点击Go
                elem.click()
                
                time.sleep(3)
                
                #定位到表体
                tbl = WebDriverWait(browser, 3).until(
                        EC.presence_of_element_located((By.ID, "dt_1")))
            except :
                dbcn.close()
                browser.quit()
                print("0出错退出")
                exit
        else:
            try:
                tbl = browser.find_element_by_id("dt_1")
            except :
                dbcn.close()
                browser.quit()
                print("1出错退出")
                exit
    
        tbody = tbl.find_element_by_tag_name('tbody')
        #表体行数
        tblrows = tbody.find_elements_by_tag_name('tr')

        #遍历行
        data = []
        sc=True     #本页处理成功
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
    
                if dm[0] in ('0','3','6') :
                    dm = lgpdm(dm)       
                    rowdat = [dm,mc,rq,yjbd,yjbdfw,yjbdfd1,yjbdfd2,yjbdfd,yglx,jlr_1]
                    rowdat = [e if e!='-' else np.nan for e in rowdat]
                    data.append(rowdat)
                    
            except:
                sc=False    #本页处理不成功
                break

        if len(data)>0 and sc:
            dbcn.executemany('''INSERT OR REPLACE INTO YJYG (GPDM,GPMC,RQ,YJBD,YJBDFW,
                                YJBDFD1,YJBDFD2,YJBDFD,YGLX,JLR_1) 
                                VALUES (?,?,?,?,?,?,?,?,?,?)''', data)
    
            dbcn.commit()
            pgn+=1
        else:
            browser.get(url)
            '''
            EC.presence_of_element_located()传递的参数是tuple元组
            '''
            elem=WebDriverWait(browser, 3).until(
                    EC.presence_of_element_located((By.ID,"PageContgopage")))            

    dbcn.close()
    browser.quit()

    now2 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now1)
    print('结束运行时间：%s' % now2)
