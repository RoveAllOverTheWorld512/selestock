# -*- coding: utf-8 -*-
"""
功能：本程序从东方财富网提取下列数据：
1、年报季报预约披露日期、预告、快报和报表的最新情况，保存sqlite
2、股东户数

用法：每天运行

和讯网 上市公司2017年年度业绩快报
http://datainfo.hexun.com/wholemarket/html/yjkb.aspx?data_type=fld_released_date&page=51
lbdq
"""
import datetime
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sqlite3
import sys
import os
import re
import numpy as np
from pyquery import PyQuery as pq
from configobj import ConfigObj


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
def wyzh(str):
    wy=re.findall('(.+)亿',str)
    if len(wy)==1 :
        return float(wy[0])*100000000
    wy=re.findall('(.+)万',str)
    if len(wy)==1 :
        return float(wy[0])*10000

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

########################################################################
#研究机构名称统一
########################################################################
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


##########################################################################
#获取运行程序所在驱动器
##########################################################################
def getdrive():
    if sys.argv[0]=='' :
        return os.path.splitdrive(os.getcwd())[0]
    else:
        return os.path.splitdrive(sys.argv[0])[0]


##########################################################################
#提取业绩报表
##########################################################################
def readyjbb(rq,html,pgn):

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

            dm = row('td').eq(1).text()     #股票代码
            mc = row('td').eq(2).text()     #股票名称
            
            eps = row('td').eq(4).text()
            yysr = wyzh(row('td').eq(5).text()) #营业收入
            yysr_g = row('td').eq(6).text()     #营业收入同比
            yysr_hb = row('td').eq(7).text()    #营业收入环比
            
            jlr = wyzh(row('td').eq(8).text())  #净利润
            jlr_g = row('td').eq(9).text()      #净利润同比
            jlr_hb = row('td').eq(10).text()    #净利润环比

            mgjzc = row('td').eq(11).text()     #每股净资产
            roe = row('td').eq(12).text()
            mgjyxjl = row('td').eq(13).text()   #每股经营现金流
            mll = row('td').eq(14).text()       #毛利率
            lrfp = row('td').eq(15).text()      #利润分配
            ggrq = row('td.col').attr('title')   #公告日期                         

            if dm[0] in ('0','3','6'):
                dm = lgpdm(dm)       
                rowdat = [dm,mc,rq,eps,yysr,yysr_g,yysr_hb,jlr,jlr_g,jlr_hb,mgjzc,roe,mgjyxjl,mll,lrfp,ggrq]
                rowdat = [e if e!='-' else np.nan for e in rowdat]
                data.append(rowdat)

        except:
            print('处理第%d页第%d行出错！' % (pgn,i))
            sc=False    #本页处理不成功
            break
    
    if len(data)>0 and sc:
        
        dbcn.executemany('''INSERT OR REPLACE INTO YJBB (GPDM,GPMC,RQ,EPS,
                            YYSR,YYSR_G,YYSR_HB,JLR,JLR_G,JLR_HB,MGJZC,ROE,MGJYXJL,MLL,LRFP,GGRQ) 
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', data)
        
        dbcn.commit()

    dbcn.close()
            
    return sc,ggrq
    
##########################################################################
#提取业绩快
##########################################################################
def readyjkb(rq,html,pgn):

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

            dm = row('td').eq(1).text()     #股票代码
            mc = row('td').eq(2).text()     #股票名称
            eps = row('td').eq(4).text()
            yysr = wyzh(row('td').eq(5).text()) #营业收入
            yysr_1 =wyzh(row('td').eq(6).text()) #营业收入（上年同期）
            yysr_g = row('td').eq(7).text()     #营业收入同比
            yysr_hb = row('td').eq(8).text()    #营业收入环比
            
            jlr = wyzh(row('td').eq(9).text())  #净利润
            jlr_1 = wyzh(row('td').eq(10).text())  #净利润（上年同期）
            jlr_g = row('td').eq(11).text()      #净利润同比
            jlr_hb = row('td').eq(12).text()    #净利润环比

            mgjzc = row('td').eq(13).text()     #每股净资产
            roe = row('td').eq(14).text()

            
            ggrq = row('td.col').attr('title')   #公告日期                         

            if dm[0] in ('0','3','6'):
                dm = lgpdm(dm)       
                rowdat = [dm,mc,rq,eps,yysr,yysr_1,yysr_g,yysr_hb,jlr,jlr_1,jlr_g,jlr_hb,mgjzc,roe,ggrq]
                rowdat = [e if e!='-' else np.nan for e in rowdat]
    
                data.append(rowdat)

        except:
            print('处理第%d页第%d行出错！' % (pgn,i))
            sc=False    #本页处理不成功
            break
    
    if len(data)>0 and sc:
        
        dbcn.executemany('''INSERT OR REPLACE INTO YJKB (GPDM,GPMC,RQ,EPS,
                            YYSR,YYSR_1,YYSR_G,YYSR_HB,JLR,JLR_1,JLR_G,JLR_HB,MGJZC,ROE,GGRQ) 
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', data)
        
        dbcn.commit()

    dbcn.close()
            
    return sc,ggrq
    
##########################################################################
#提取业预告
##########################################################################
def readyjyg(rq,html,pgn):

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


            dm = row('td').eq(1).text()         #股票代码
            mc = row('td').eq(2).text()         #股票名称

            yjbd = row('td').eq(4).text()       #业绩变动
            yjjlr=row('td').eq(5).text()
            if '～' in yjjlr:
                yjjlr=yjjlr.replace('～','+')
                yjjlr=yjjlr.replace('万','*10000')
                yjjlr=yjjlr.replace('亿','*100000000')
                jlr = eval(yjjlr)/2    
            else:
                jlr = wyzh(row('td').eq(5).text())  #预计净利润    
            
            
            yjbdfw = row('td').eq(6).text()     #业绩变动范围
            yglx = row('td').eq(8).text()       #预告类型
            jlr_1 = wyzh(row('td').eq(9).text()) #上年同期净利润
               
            ggrq = row('td.col').attr('title')   #公告日期                         
            
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
                

            if dm[0] in ('0','3','6') :
                dm = lgpdm(dm)       
                rowdat = [dm,mc,rq,yjbd,yjbdfw,yjbdfd1,yjbdfd2,yjbdfd,yglx,jlr_1,jlr,ggrq]
                rowdat = [e if e!='-' else np.nan for e in rowdat]
                data.append(rowdat)


        except:
            print('处理第%d页第%d行出错！' % (pgn,i))
            sc=False    #本页处理不成功
            break
    
    if len(data)>0 and sc:
        
        dbcn.executemany('''INSERT OR REPLACE INTO YJYG (GPDM,GPMC,RQ,YJBD,YJBDFW,
                            YJBDFD1,YJBDFD2,YJBDFD,YGLX,JLR_1,jlr,ggrq) 
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''', data)
        
        dbcn.commit()

    dbcn.close()
            
    return sc,ggrq
    

##########################################################################
#提取业绩预约披露时间
##########################################################################
def readyysj(rq,html,pgn):

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

            dm = row('td').eq(0).text()         #股票代码
            yyrq = row('td').eq(3).text()       #首次预约时间
            yyrq1 = row('td').eq(4).text()       #一次变更日期
            yyrq2 = row('td').eq(5).text()       #二次变更日期
            yyrq3 = row('td').eq(6).text()       #三次变更日期
            if yyrq1!='-':
                yyrq=yyrq1
            if yyrq2!='-':
                yyrq=yyrq2
            if yyrq3!='-':
                yyrq=yyrq3
    
            if dm[0] in ('0','3','6') and yyrq!='-':
    
                dm = lgpdm(dm)       
                data.append([dm,yyrq])
        
        except:
            print('处理第%d页第%d行出错！' % (pgn,i))
            sc=False    #本页处理不成功
            break
    
    if len(data)>0 and sc:
        
        dbcn.executemany('''INSERT OR REPLACE INTO YYSJ (GPDM,YYRQ) 
                            VALUES (?,?)''', data)
        
        dbcn.commit()

    dbcn.close()
            
    return sc,None
    

##########################################################################
#提取股东户数
##########################################################################
def readgdhs(html,pgn):

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

            dm = row('td').eq(0).text()
            hs = row('td').eq(5).text()
            rq = row('td').eq(10).text()

            ggrq = pq(row('td.col'))('span').attr('title')
            ggrq=ggrq.replace('/','-')
 
            if dm[0] in ('0','3','6') and rq != None and hs != None :
                dm = lgpdm(dm)       
                rq = rq.replace('/','-')
                rowdat = [dm,rq,hs,ggrq]
                data.append(rowdat)


        except:
            print('处理第%d页第%d行出错！' % (pgn,i))
            sc=False    #本页处理不成功
            break
    
    if len(data)>0 and sc:
        
        dbcn.executemany('INSERT OR REPLACE INTO GDHS (GPDM,RQ,GDHS,GGRQ) VALUES (?,?,?,?)', data)
        
        dbcn.commit()

    dbcn.close()
            
    return sc,ggrq
    
##########################################################################
#提取最新研报
##########################################################################
def readzxyb(html,pgn):

    pjjgdic=jgdic()
    url0='http://data.eastmoney.com'
    nf1='2018预测'
    nf2='2019预测'
    
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
            dm = row('td').eq(2).text()     #股票代码
            rq = pq(row('td').eq(1))('span').attr('title')  #公告日期
            bt = pq(row('td').eq(5))('a').attr('title')     #研报标题
            wz = url0+pq(row('td').eq(5))('a').attr('href') #研报网址
            pj = row('td').eq(6).text()                     #原文评级
            pjbd = row('td').eq(7).text()                   #评级变动
            pjjg = row('td').eq(8).text()                   #评级机构
            #评级机构名称统一化
            if pjjg in pjjgdic.keys():
                pjjg=pjjgdic[pjjg]

            eps1 = row('td').eq(9).text()
            eps2 = row('td').eq(11).text()
 
            if dm[0] in ('0','3','6'):
                dm = lgpdm(dm)       
                rowdat = [dm,rq,bt,wz,pj,pjbd,pjjg,nf1,eps1,nf2,eps2]
                rowdat = [e if e!='-' else np.nan for e in rowdat]

                data.append(rowdat)

        except:
            print('处理第%d页第%d行出错！' % (pgn,i))
            sc=False    #本页处理不成功
            break
    
    if len(data)>0 and sc:
        
        dbcn.executemany('''INSERT OR REPLACE INTO REPORT (GPDM,RQ,YBBT,WZ,PJ,
                            PJBD,PJJG,NF1,EPS1,NF2,EPS2) 
                            VALUES (?,?,?,?,?,?,?,?,?,?,?)''', data)
        
        dbcn.commit()

    dbcn.close()
            
    return sc,rq
    

##########################################################################
#提取业绩报表
##########################################################################
def readeastmoney(rq,url,lb,lbmc):


    fireFoxOptions = webdriver.FirefoxOptions()
    fireFoxOptions.set_headless()
    browser = webdriver.Firefox(firefox_options=fireFoxOptions)

#    browser = webdriver.Firefox()

    #下面的语句不行
#    chrome_options = webdriver.ChromeOptions() 
#    chrome_options.add_argument("--headless") 
#    chrome_options.add_argument('--disable-gpu')
#    browser = webdriver.Chrome(chrome_options=chrome_options) 


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

    while pgn<=pgs:

        print("正在处理【%s】第%d/%d页，请等待。" % (lbmc,pgn,pgs))
        if pgn>1:
            try :   
                if lb=='zxyb':
                    elem = browser.find_element_by_id("gopage")
                else:
                    elem = browser.find_element_by_id("PageContgopage")
                    
                elem.clear()
                #输入页面
                elem.send_keys(pgn)
                elem = browser.find_element_by_class_name("btn_link")     
                #点击Go
                elem.click()
                                
                #定位到表体,可能dt_1已调入，但表体数据没有完成调入，只有一行“数据加载中...”
                tbl = WebDriverWait(browser, 3).until(
                        EC.presence_of_element_located((By.ID, "dt_1")))
                tbody = WebDriverWait(tbl, 10).until(
                        EC.presence_of_element_located((By.TAG_NAME, "tbody")))
            except :
                browser.quit()
                print("0出错退出")
                sys.exit()
        else:
            try:
                tbl = WebDriverWait(browser, 3).until(
                        EC.presence_of_element_located((By.ID, "dt_1")))
                tbody = WebDriverWait(tbl, 10).until(
                        EC.presence_of_element_located((By.TAG_NAME, "tbody")))

            except :
                browser.quit()
                print("1出错退出")
                sys.exit()

#        tbody = tbl.find_element_by_tag_name('tbody')
        html=tbody.get_attribute('innerHTML')

        if lb=='yjbb':
            sc,ggrq=readyjbb(rq,html,pgn)
        elif lb=='yjkb' :
            sc,ggrq=readyjkb(rq,html,pgn)
        elif lb=='yjyg' :
            sc,ggrq=readyjyg(rq,html,pgn)
        elif lb=='yysj' :
            sc,ggrq=readyysj(rq,html,pgn)
        elif lb=='gdhs' :
            sc,ggrq=readgdhs(html,pgn)
        elif lb=='zxyb' :
            sc,ggrq=readzxyb(html,pgn)
        else :
            print('调用类别错误！')
            sys.exit()            
            
        if sc:
            pgn += 1
            
        else:
            try:
                browser.get(url)
                '''
                EC.presence_of_element_located()传递的参数是tuple元组
                '''
                if lb=='zxyb':
                    elem=WebDriverWait(browser, 10).until(
                        EC.presence_of_element_located((By.ID,"gopage")))            
                else:
                    elem=WebDriverWait(browser, 10).until(
                        EC.presence_of_element_located((By.ID,"PageContgopage")))            
            except:
                browser.quit()
                print("2出错退出")
                sys.exit()
                

        if ggrq!=None and ggrq<lastdate:
            break
    
    browser.quit()    

    return
    

if __name__ == "__main__": 
    print('%s Running' % sys.argv[0])
    now1 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now1)

    today=datetime.datetime.now().strftime('%Y-%m-%d')
    config = iniconfig()
    lastdate = readkey(config,'lastdate','2018-07-01')

    rq = '2018.06.30'
    bgq=rq[:4]+rq[5:7]
    items=[['yjbb',"http://data.eastmoney.com/bbsj/%s/yjbb.html" % bgq,'业绩报表'],
           ['yjkb',"http://data.eastmoney.com/bbsj/%s/yjkb/13.html" % bgq,'业绩快报'],
           ['yjyg',"http://data.eastmoney.com/bbsj/%s/yjyg.html" % bgq,'业绩预告'],
           ['yysj','http://data.eastmoney.com/bbsj/%s/yysj.html' % bgq,'业绩披露预约时间'],
           ['gdhs','http://data.eastmoney.com/gdhs/','股东户数'],
           ['zxyb','http://data.eastmoney.com/report/','最新研报']]
    
#    for lb,url,lbmc in items:
    lb,url,lbmc = ['zxyb','http://data.eastmoney.com/report/','最新研报']
    readeastmoney(rq,url,lb,lbmc)

    config['lastdate'] = today
    config.write()
    
    now2 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now1)
    print('结束运行时间：%s' % now2)
