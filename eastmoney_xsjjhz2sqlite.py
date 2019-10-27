# -*- coding: utf-8 -*-
"""
功能：本程序从东方财富网提取股东户数的最新变化情况，保存sqlite
用法：每天运行
"""
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sqlite3
import sys
from pyquery import PyQuery as pq
import traceback
import datetime


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
#建立数据库
########################################################################
def createDataBase():
    dbfn=getdrive()+'\\hyb\\STOCKDATA.db'
    cn = sqlite3.connect(dbfn)

    cn.execute('''CREATE TABLE IF NOT EXISTS GDHS
           (GPDM TEXT NOT NULL,
           RQ TEXT NOT NULL,
           GDHS INTEGER NOT NULL);''')
    cn.execute('''CREATE UNIQUE INDEX IF NOT EXISTS GPDM_RQ_GDHS ON GDHS(GPDM,RQ);''')



def get_xsjjhz(pgn):

    data = []
    
    print("正在处理第%d页，请等待。" % pgn)
    dbfn=getdrive()+'\\hyb\\STOCKDATA.db'
    dbcn = sqlite3.connect(dbfn)

    browser = webdriver.PhantomJS()
    browser.get("http://data.eastmoney.com/dxf/detail.aspx?market=0")
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
        return False

    #遍历行
    for j in range(len(tblrows)):
        dm = None
        jjrq = None
        bcjj = None
        qltzb = None
        try :    
            tblcols = tblrows[j].find_elements_by_tag_name('td')
            dm = tblcols[1].text
            jjrq = tblcols[4].text
            bcjj =  tblcols[6].text
            qltzb = tblcols[8].text
        except :
            dbcn.close()
            return False

        dm = lgpdm(dm)       
        rowdat = [dm,jjrq,bcjj,qltzb]
        data.append(rowdat)

    browser.quit()
    
    if len(data)>0:
        dbcn.executemany('INSERT OR REPLACE INTO XSJJ_DFCF (GPDM,JJRQ,JJSL,QLTBL) VALUES (?,?,?,?)', data)
        
    dbcn.commit()
    dbcn.close()

    return True

def getdrive():
    return sys.argv[0][:2]


def str2num(s):
    s=s.replace('万','*10000')
    s=s.replace('亿','*100000000')
    try:
        s = eval(s)/10000
    except:
        s = None
        
    return s


##########################################################################
#提取限售解禁汇总
##########################################################################
def readxsjjhz(html,pgn):

    '''
    股票代码、股票名称、解禁日期、限售股类型、解禁数量、实际解禁、解禁市值、占解禁前流通比例、
    解禁前20个交易日涨跌、解禁后20个交易日涨跌
    CREATE TABLE [XSJJ_DFCF](
      [GPDM] TEXT NOT NULL, 
      [GPMC] TEXT, 
      [JJRQ] TEXT NOT NULL, 
      [XSGLX] TEXT, 
      [JJSL] TEXT NOT NULL, 
      [SJJJ] REAL, 
      [JJSZ] REAL, 
      [QLTBL] REAL,
      [QSP] REAL, 
      [Q20ZF] REAL,
      [H20ZF] REAL
      );
    
    CREATE UNIQUE INDEX [GPDM_JJRQ_XSJJ_DFCF]
    ON [XSJJ_DFCF](
      [GPDM], 
      [JJRQ]);

    '''
    
    dbfn=getdrive()+'\\hyb\\STOCKDATA.db'
    dbcn = sqlite3.connect(dbfn)
    
    html=pq(html)

    html.find("script").remove()    # 清理 <script>...</script>
    html.find("style").remove()     # 清理 <style>...</style>

    rows=html('tr')
    '''
    Python中的jquery PyQuery库使用小结
    https://www.jb51.net/article/50069.htm        

    '''
    #遍历行
    data = []
    sc=True     #本页处理成功
    for i in range(len(rows)):

        try:

            row=pq(rows('tr').eq(i))

            dm = row('td').eq(1).text()
            mc = row('td').eq(2).text()
            jjrq = row('td').eq(4).text()
            xsglx = row('td').eq(5).text()
            
            bcjj = str2num(row('td').eq(6).text())
            sjjj = str2num(row('td').eq(7).text())
            jjsz = str2num(row('td').eq(8).text())
            
            qltzb = row('td').eq(9).text()
            qltzb = None if qltzb=='-' else qltzb

            qsp = row('td').eq(10).text()
            
            q20zf = row('td').eq(11).text()
            q20zf = None if q20zf=='-' else q20zf
            
            h20zf = row('td').eq(12).text()
            h20zf = None if h20zf=='-' else h20zf
            
            dm = lgpdm(dm)       
            rowdat = [dm,mc,jjrq,xsglx,bcjj,sjjj,jjsz,qltzb,qsp,q20zf,h20zf]
            data.append(rowdat)


        except:
            print('处理第%d页第%d行出错！' % (pgn,i))
            sc=False    #本页处理不成功
            break
    
    if len(data)>0 and sc:
        
        dbcn.executemany('''INSERT OR REPLACE INTO XSJJ_DFCF 
                         (GPDM,GPMC,JJRQ,XSGLX,JJSL,SJJJ,JJSZ,QLTBL,QSP,Q20ZF,H20ZF) 
                         VALUES (?,?,?,?,?,?,?,?,?,?,?)''', data)
        
        dbcn.commit()

    dbcn.close()
            
    return sc
    


if __name__ == "__main__": 
    print('%s Running' % sys.argv[0])

    td=datetime.datetime.now()

    nw=td.strftime("%Y-%m-%d")
    dt1=(td+datetime.timedelta(-365*2)).strftime("%Y-%m-%d")
    dt2=(td+datetime.timedelta(365*2)).strftime("%Y-%m-%d")

    url='http://data.eastmoney.com/dxf/detail.aspx?market=all'
    lbmc='解禁详情一览 '
    lb='xsjjhz'
    
    fireFoxOptions = webdriver.FirefoxOptions()
    fireFoxOptions.set_headless()
    browser = webdriver.Firefox(firefox_options=fireFoxOptions)

#    browser = webdriver.Firefox()


    browser.get(url)
    time.sleep(3)

        
#    elem=WebDriverWait(browser, 20).until(
#        EC.presence_of_element_located((By.ID, "search_date_start")))

    '''
    pythonjs 处理日历控件（修改readonly属性）
    https://www.cnblogs.com/mojiemeizi/p/7738048.html
    
    '''
    js = "document.getElementById('search_date_start').removeAttribute('readonly')"
    browser.execute_script(js)
    
    elem = browser.find_element_by_id("search_date_start")     
    elem.clear()
    elem.send_keys(dt1)

#    elem=WebDriverWait(browser, 20).until(
#        EC.presence_of_element_located((By.ID, "search_date_end")))
    
    js = "document.getElementById('search_date_end').removeAttribute('readonly')"
    browser.execute_script(js)

    elem = browser.find_element_by_id("search_date_end")     
    elem.clear()
    elem.send_keys(dt2)

#    elem=WebDriverWait(browser, 20).until(
#        EC.presence_of_element_located((By.CLASS_NAME,"search_btn")))

    elem = browser.find_element_by_class_name("search_btn")     

    elem.click()
    time.sleep(10)

    try:
        elem = browser.find_element_by_xpath("//a[text()='下一页']//preceding-sibling::a[1]")
        pgs=int(elem.text)
    except:
        pgs=1

    pgn=1   
    errcs=0
    while pgn<=pgs:

        print("正在处理【%s】第%d/%d页，请等待。" % (lbmc,pgn,pgs))
        if pgn>1:
            try :   
                elem = WebDriverWait(browser, 30).until(
                    EC.presence_of_element_located((By.ID, "PageContgopage")))                    

                elem.clear()
                #输入页面
                elem.send_keys(pgn)
                elem = browser.find_element_by_class_name("btn_link")     
                #点击Go
                elem.click()

                
                #定位到表体,可能dt_1已调入，但表体数据没有完成调入，只有一行“数据加载中...”
                tbl = WebDriverWait(browser, 20).until(
                        EC.presence_of_element_located((By.ID, "td_1")))
                
                WebDriverWait(tbl, 20).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "col")))

                tbl = browser.find_element_by_id('td_1')
            except :
                print("0出错退出")
                errcs += 1
                if errcs<5:
                    browser.get(url)
                    continue
                else:
                    browser.quit()
                    f=open(r"d:\selestock\log.txt",'a')  
                    traceback.print_exc(file=f)  
                    f.flush()  
                    f.close()

                    print("0出错退出")
                    sys.exit()
        else:
            try:
                tbl = WebDriverWait(browser, 20).until(
                        EC.presence_of_element_located((By.ID, "td_1")))
                
                WebDriverWait(tbl, 20).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "col")))

                tbl = browser.find_element_by_id('td_1')
            except :
                browser.quit()
                f=open(r"d:\selestock\log.txt",'a')  
                traceback.print_exc(file=f)  
                f.flush()  
                f.close()

                print("1出错退出")
                sys.exit()

        tbody = tbl.find_element_by_tag_name('tbody')
        html=tbody.get_attribute('innerHTML')

        sc=readxsjjhz(html,pgn)
            
        if sc:
            
            pgn += 1
            errcs = 0
           
    
    browser.quit()    

    
