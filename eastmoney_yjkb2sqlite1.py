# -*- coding: utf-8 -*-
"""
功能：本程序从东方财富网提取股东户数的最新变化情况，保存sqlite
用法：每天运行
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
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    cn = sqlite3.connect(dbfn)
    '''
    股票代码,日期，每股收益(元),营业收入(元),营业收入去年同期(元),营业收入同比增长(%),营业收入季度环比增长(%),净利润
    (元),净利润去年同期(元),净利润同比增长(%),净利润季度环比增长(%),每股净资产(元),净资产收益率(%),公告日期
    '''
    cn.execute('''CREATE TABLE IF NOT EXISTS YJKB
           (GPDM TEXT NOT NULL,
           RQ TEXT NOT NULL,
           EPS REAL,
           YYSR REAL,
           YYSR_1 REAL,
           YYSR_G REAL,
           YYSR_HB REAL,
           JLR REAL,
           JLR_1 REAL,
           JLR_G REAL,
           JLR_HB REAL,
           MGJZC REAL,
           ROE REAL
           );''')
    cn.execute('''CREATE UNIQUE INDEX IF NOT EXISTS YJKB_RQ_GDHS ON YJKB(GPDM,RQ);''')



def getyjkb(pgn):
    
    print("正在处理第%d页，请等待。" % pgn)
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)

    browser = webdriver.PhantomJS()
    browser.get("http://data.eastmoney.com/bbsj/201712/yjkb/13.html")
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
        browser.quit()
        print("1出错退出")
        return False

    #遍历行
    data = []
    for j in range(len(tblrows)):
        
        try :    
            tblcols = tblrows[j].find_elements_by_tag_name('td')
            '''
            股票代码,日期，每股收益(元),营业收入(元),营业收入去年同期(元),
            营业收入同比增长(%),营业收入季度环比增长(%),净利润(元),
            净利润去年同期(元),净利润同比增长(%),净利润季度环比增长(%),
            每股净资产(元),净资产收益率(%)
            '''
            
            dm = tblcols[1].text
            eps = tblcols[4].text
            yysr = wyzh(tblcols[5].text)
            yysr_1 = wyzh(tblcols[6].text)
            yysr_g = tblcols[7].text
            yysr_hb = tblcols[8].text
            
            jlr = wyzh(tblcols[9].text)
            jlr_1 = wyzh(tblcols[10].text)
            jlr_g = tblcols[11].text
            jlr_hb = tblcols[12].text

            mgjzc = tblcols[13].text
            roe = tblcols[14].text
                        
        except :
            dbcn.close()
            browser.quit()
            print("2出错退出,请检查行号：",j)
            return False

        if dm[0] in ('0','3','6') and eps!='-' and jlr_g!='-':

            dm = lgpdm(dm)       
            rq = '2017.12.31'
            rowdat = [dm,rq,eps,yysr,yysr_1,yysr_g,yysr_hb,jlr,jlr_1,jlr_g,jlr_hb,mgjzc,roe]
            rowdat = [e if e!='-' else np.nan for e in rowdat]

            data.append(rowdat)
    
    dbcn.executemany('''INSERT OR REPLACE INTO YJKB (GPDM,RQ,EPS,
                        YYSR,YYSR_1,YYSR_G,YYSR_HB,JLR,JLR_1,JLR_G,JLR_HB,MGJZC,ROE) 
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''', data)
    dbcn.commit()
    dbcn.close()

    browser.quit()
    return True

def getdrive():
    return sys.argv[0][:2]

def main():
    createDataBase()
    j=1
    while j<=2:
        if getyjkb(j):
            j+=1


if __name__ == "__main__": 
    print('%s Running' % sys.argv[0])
    main()