# -*- coding: utf-8 -*-
"""
功能：本程序从东方财富网提取下列数据：
1、概念板块名称、代码及包含的股票，保存sqlite
2、股东户数

用法：每天运行

和讯网 上市公司2017年年度业绩快报
http://datainfo.hexun.com/wholemarket/html/yjkb.aspx?data_type=fld_released_date&page=51
lbdq

Python之系统交互（subprocess）
https://www.cnblogs.com/yyds/p/7288916.html


"""
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sqlite3
import sys
import os
import re
from pyquery import PyQuery as pq
from configobj import ConfigObj
import traceback
import tushare as ts
import pandas as pd

########################################################################
#初始化本程序配置文件
########################################################################
def iniconfig():
    inifile = os.path.splitext(sys.argv[0])[0]+'.ini'  #设置缺省配置文件
    return ConfigObj(inifile,encoding='GBK')


#########################################################################
#读取键值,如果键值不存在，就设置为defvl
#########################################################################
def readkey(config,key,defvl=None):
    keys = config.keys()
    if defvl==None :
        if keys.count(key) :
            return config[key]
        else :
            return ""
    else :
        if not keys.count(key) :
            config[key] = defvl
            config.write()
            return defvl
        else:
            return config[key]


###############################################################################
#万亿转换
###############################################################################
def wyzh(s):
    s =s.replace('万','*10000').replace('亿','*100000000')
    try:
        return eval(s)
    except:
        return None

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


##########################################################################
#获取运行程序所在驱动器
##########################################################################
def getdrive():
    if sys.argv[0]=='' :
        return os.path.splitdrive(os.getcwd())[0]
    else:
        return os.path.splitdrive(sys.argv[0])[0]



##########################################################################
#板块资金
##########################################################################
def readbkzj(html,pgn,rq):
    
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
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

            bkmc = pq(row('td').eq(1))('a').text()            #板块名称
            bkwz = pq(row('td').eq(1))('a').attr('href')      #网址
            
            bkdm=re.findall('#(.*?)_0_2',bkwz)
            bkdm1=bkdm[0]                

            wz = pq(row('td').eq(2))('a').attr('href')      #网址
            bkdm=re.findall('/BK(.*?)\.html',wz)                
            bkdm2='BK'+bkdm[0]                

            data.append([rq,bkmc,bkwz,bkdm1,bkdm2])
                            
        except:
            print('处理第%d页第%d行出错！' % (pgn,i))
            sc=False    #本页处理不成功
            break

    
    if len(data)>0 and sc:
        
        dbcn.executemany('''INSERT OR REPLACE INTO DFCFWGN (RQ,BKMC,BKWZ1,BKDM1,BKDM2) 
                            VALUES (?,?,?,?,?)''', data)
        
        dbcn.commit()

    dbcn.close()
            
    return sc
    

##########################################################################
#板块
##########################################################################
def readbkgpdms(rq):
    
    fireFoxOptions = webdriver.FirefoxOptions()
    fireFoxOptions.headless=True
    browser = webdriver.Firefox(options=fireFoxOptions)

    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)

    curs = dbcn.cursor()
    sql='''select rq,bkmc,bkwz1,bkdm1,bkdm2 from dfcfwgn 
               where rq="%s" and gps is NULL order by bkdm2 
            ;'''  % rq
    curs.execute(sql)        
    data = curs.fetchall()

    data1=[]

    bks=len(data)
    for j in range(bks):
        rq,bkmc,bkwz1,bkdm1,bkdm2 = data[j]
        print(bkmc,bkwz1)

        for i in range(5):
            ok,gps,gpdms,bkwz2=readbkgp(browser,bkwz1)            
            if ok:
                data1.append([rq,bkmc,bkwz1,bkdm1,bkdm2,gps,gpdms,bkwz2])
                j=j+1
                break
            
        if j%5==0 or j==bks-1:
            dbcn.executemany('''INSERT OR REPLACE INTO DFCFWGN (RQ,BKMC,BKWZ1,BKDM1,BKDM2,
                        GPS,GPDMS,BKWZ2) 
                        VALUES (?,?,?,?,?,?,?,?)''', data1)
            
            dbcn.commit()
            data1=[]
            
    dbcn.close()

    browser.quit()

    return 

##########################################################################
#板块
##########################################################################
def readgpgn(rq):

    df=ts.get_stock_basics()
    df=df[['name']]
    df['gpdm']=df.index.map(lgpdm)
        
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)

    curs = dbcn.cursor()
    sql='''select rq,bkmc,gpdms from dfcfwgn 
               where rq="%s" order by bkdm2 
            ;'''  % rq
    curs.execute(sql)        
    data = curs.fetchall()

    data1={}

    bks=len(data)
    for j in range(bks):
        rq,bkmc,gpdms = data[j]

        gpdms=gpdms.split('|')

        gpdms=list(set(gpdms))  #去重
        
        for gpdm in gpdms:
            if gpdm in df.index:
                dm=lgpdm(gpdm)

                if dm in data1.keys():
                    data1[dm][1]=data1[dm][1]+','+bkmc
                    data1[dm][0]=data1[dm][0]+1
                else:
                    data1[dm]=[1,bkmc]
                    
    df2=pd.DataFrame.from_dict(data1, orient='index',columns=['gns','gnmc'])
    df2.index.name='gpdm'

    df1=df.set_index('gpdm',drop=False)
    df1['rq']=rq
    df1=df1[['rq','gpdm','name']]
    df1.columns=['rq','gpdm','gpmc']

    df=df1.join(df2)
    
    df=df.loc[(~df['gns'].isna())]

    data=df.values.tolist()

    if len(data)>0:
        dbcn.executemany('''INSERT OR REPLACE INTO DFCFWGPGN (RQ,GPDM,GPMC,GNGS,GNMC)
                    VALUES (?,?,?,?,?)''', data)
        
        dbcn.commit()
            
    dbcn.close()


    return 

##########################################################################
#板块
##########################################################################
def readbkgp(browser,url):
    
    sc = True
    data = '|'      #股票代码串
    gps = 0           #股票数
    browser.get(url)
    try:
        '''
        EC.presence_of_element_located()传递的参数是tuple元组
        '''

        elem=WebDriverWait(browser, 20).until(
            EC.presence_of_element_located((By.ID, "main-table_paginate_page")))

        a=elem.find_elements_by_class_name('paginate_button')
        pgs=int(a[len(a)-1].text)
        
    except:
        pgs=1
    
#    print("pgs=%d" % pgs)
    
    pgn=1   
    while pgn<=pgs:

        if pgn>1:
            elem = browser.find_element_by_class_name("paginate_input")     
            elem.clear()
            #输入页面
            elem.send_keys(pgn)
            #点击Go
            
            elem = browser.find_element_by_class_name("paginte_go")     
        
            elem.click()

            
        try:
            tbl = WebDriverWait(browser, 30).until(
                    EC.presence_of_element_located((By.ID, "main-table")))

            tbody = tbl.find_element_by_tag_name('tbody')
            html=tbody.get_attribute('innerHTML')
        
        except:
            sc=False
            break
            
        
        html=pq(html)
    
        html.find("script").remove()    # 清理 <script>...</script>
        html.find("style").remove()     # 清理 <style>...</style>

        rows=html('tr')

        #遍历行
        for i in range(len(rows)):
            row=pq(rows('tr').eq(i))
            dm = row('td').eq(1).text()     #股票代码
            data = data+dm+'|'
            gps += 1
            
        pgn += 1

    return sc,gps,data,browser.current_url

##########################################################################
#提取业绩报表
##########################################################################
def readeastmoney(rq,url,lb,lbmc):

    fireFoxOptions = webdriver.FirefoxOptions()
    fireFoxOptions.headless=True
    browser = webdriver.Firefox(options=fireFoxOptions)

    browser.get(url)


    try:
        '''
        EC.presence_of_element_located()传递的参数是tuple元组
        '''
        elem=WebDriverWait(browser, 20).until(
            EC.presence_of_element_located((By.XPATH, "//a[text()='下一页']//preceding-sibling::a[1]")))
        pgs=int(elem.text)
    except:
        tbl = browser.find_element_by_id("dt_1").get_attribute('innerHTML')
        if '没有相关数据' in tbl :
            browser.quit()    
            return
        else:
            pgs=1

    pgn=1   
    errcs=0
    while pgn<=pgs:
        
        print("正在处理【%s_%s】第%d/%d页，请等待。" % (rq,lbmc,pgn,pgs))

#        print("正在处理【%s】第%d/%d页，请等待。" % (lbmc,pgn,pgs))
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
                        EC.presence_of_element_located((By.ID, "dt_1")))
                
                WebDriverWait(tbl, 20).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "odd")))

                tbl = browser.find_element_by_id('dt_1')

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
                        EC.presence_of_element_located((By.ID, "dt_1")))
                
                
                WebDriverWait(tbl, 20).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "odd")))

                tbl = browser.find_element_by_id('dt_1')

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

            
        if lb=='bkzj' :
            
            sc=readbkzj(html,pgn,rq)
        else :
            print('调用类别错误！')
            sys.exit()            
            
        if sc:
            
            pgn += 1
            errcs = 0
           

        browser.get(url)

    
    browser.quit()    

    return

        
if __name__ == "__main__": 
    
#    sys.exit()
    
    print('%s Running' % sys.argv[0])
    now1 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now1)

    rq = datetime.datetime.now().strftime('%Y.%m.%d')

    lb,url,lbmc = ['bkzj','http://data.eastmoney.com/bkzj/gn.html','板块资金']
    readeastmoney(rq,url,lb,lbmc)

#
#    readbkgpdms(rq)
    
    now2 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now1)
    print('结束运行时间：%s' % now2)
