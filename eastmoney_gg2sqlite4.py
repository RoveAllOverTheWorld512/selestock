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
import pandas as pd
import traceback
 
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
    s=s.replace('万','*10000').replace('亿','*100000000')
    try:
        return eval(s)
    except:
        return None


###############################################################################
#字符串转数值
###############################################################################
def str2num(s):
    if '.' in s:
        try:
            return round(float(s),4)
        except:
            return None
    else:
        try:
            return int(s)
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

    if '没有相关数据' in html:
        print('没有相关数据,退出')
        return True

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

    if '没有相关数据' in html:
        print('没有相关数据,退出')
        return True

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
#提取个股研报列表
##########################################################################
def readggyb(dm,html,pgn):
    '''
    CREATE TABLE [DFCFYB](
      [GPDM] TEXT NOT NULL, 
      [RQ] TEXT NOT NULL, 
      [PJLB] TEXT NOT NULL, 
      [PJBD] TEXT, 
      [PJJG] TEXT NOT NULL, 
      [YBBT] TEXT, 
      [YBWZ] TEXT);
    
    CREATE UNIQUE INDEX [GPDM_RQ_PJJG_DFCFYB]
    ON [DFCFYB](
      [GPDM], 
      [RQ], 
      [PJJG]);
    
    CREATE TABLE [DFCF](
      [GPDM] TEXT NOT NULL, 
      [RQ] TEXT NOT NULL, 
      [TS1] TEXT, 
      [TS2] TEXT, 
      [TSLX] TEXT NOT NULL);
    
    CREATE UNIQUE INDEX [GPDM_RQ_TS1_TS2_DFCF]
    ON [DFCF](
      [GPDM], 
      [RQ], 
      [TS1], 
      [TS2]);
    
    
    '''
    if '没有相关数据' in html:
        print('没有相关数据,退出')
        return True

    pjjgdic=jgdic()
    url0='http://data.eastmoney.com'

    dbfn=getdrive()+'\\hyb\\STOCKDSTX.db'
    dbcn = sqlite3.connect(dbfn)
    
    html=pq(html)

    html.find("script").remove()    # 清理 <script>...</script>
    html.find("style").remove()     # 清理 <style>...</style>

    rows=html('ul')
    '''
    Python中的jquery PyQuery库使用小结
    https://www.jb51.net/article/50069.htm        

    '''
    tslx='3'
    dm = lgpdm(dm) 
    #遍历行
    data = []
    data1 = []
    sc=True     #本页处理成功
    for i in range(len(rows)):

        try:

            row=pq(rows.eq(i))


            rq = row('li').eq(0).text()             #日期
            pjlb = row('li').eq(1).text()           #评级类别
            pjbd = row('li').eq(2).text()           #评级变动
            pjjg = row('li').eq(3).text()           #评级机构
            #评级机构名称统一化
            if pjjg in pjjgdic.keys():
                pjjg=pjjgdic[pjjg]

            ybbt = row('li').eq(4).text()           #研报标题
            ybwz = url0+pq(row('li').eq(4))('a').attr('href') #研报网址


            if len(pjlb)>0:
                rowdat = [dm,rq,pjlb,pjbd,pjjg,ybbt,ybwz]
                rowdat = [e if e!='-' else np.nan for e in rowdat]
                data.append(rowdat)
    
                ts1='%s[%s,%s]' % (pjlb,pjjg,pjbd)
                ts2='%s[%s]%s' % (pjlb,pjjg,ybbt)
    
                rowdat = [dm,rq,ts1,ts2,tslx]
                data1.append(rowdat)

        except:
            print('处理第%d页第%d行出错！' % (pgn,i))
            sc=False    #本页处理不成功
            break
    
    if  sc:
        
        dbcn.executemany('''INSERT OR REPLACE INTO DFCFYB (GPDM,RQ,PJLB,PJBD,
                            PJJG,YBBT,YBWZ) 
                            VALUES (?,?,?,?,?,?,?)''', data)
        
        dbcn.executemany('''INSERT OR REPLACE INTO DFCF (GPDM,RQ,TS1,TS2,TSLX) 
                            VALUES (?,?,?,?,?)''', data1)
        dbcn.commit()

    dbcn.close()
            
    return sc
    

##########################################################################
#提取高管持股变动
##########################################################################
def readggcgbd(dm,html,pgn):
    '''
    股票代码，变动日期，变动高管名称，变动股数，变动方向，成交均价，
    成交金额，变动类型（变动原因）,总股本占比，变动后持股，相关高管名称，
    高管职务，与高管关系
    CREATE TABLE [DFCFGGCGBD](
      [GPDM] TEXT NOT NULL, 
      [BDRQ] TEXT NOT NULL, 
      [GGMC] TEXT NOT NULL, 
      [BDGS] REAL NOT NULL, 
      [BDFX] TEXT NOT NULL, 
      [BDJJ] REAL, 
      [BDSZ] REAL, 
      [BDLX] TEXT, 
      [ZGBZB] REAL, 
      [BDHCG] REAL, 
      [XGGG] TEXT NOT NULL, 
      [GGZW] TEXT, 
      [YGGGX] TEXT);
    
    CREATE UNIQUE INDEX [GPDM_BDRQ_GGMC_DFCFGGCGBD]
    ON [DFCFGGCGBD](
      [GPDM], 
      [BDRQ], 
      [GGMC]);

    '''

    if '没有相关数据' in html:
        print('没有相关数据,退出')
        return True

    dbfn=getdrive()+'\\hyb\\STOCKDSTX.db'
    dbcn = sqlite3.connect(dbfn)
    
    html=pq(html)

    html.find("script").remove()    # 清理 <script>...</script>
    html.find("style").remove()     # 清理 <style>...</style>

    rows=html('tr')
    '''
    Python中的jquery PyQuery库使用小结
    https://www.jb51.net/article/50069.htm        

    '''
    dm = lgpdm(dm)       
    tslx='2'
    #遍历行
    data = []
    data1 = []
    sc=True     #本页处理成功
    for i in range(len(rows)):

        try:

            row=pq(rows.eq(i))

            bdrq = row('td').eq(0).text()       #变动日期
            ggmc = row('td').eq(3).text()       #变动人
            bdgs = row('td').eq(4).text()       #变动股数
            bdgs = eval(bdgs.replace('万','*10000').replace('亿','*100000000'))
            bdfx = '增持' if bdgs>0 else '减持'
            bdjj = row('td').eq(5).text()       #变动均价
                 
            bdsz = row('td').eq(6).text()      #变动金额，成交市值
            bdlx = row('td').eq(7).text()      #变动原因，
            zgbzb = row('td').eq(8).text()      #总股本占比%
            bdhcg = row('td').eq(9).text()       #变动持股股数
            try :
                bdhcg = eval(bdhcg.replace('万','*10000').replace('亿','*100000000'))
            except:
                bdhcg = None
            xggg = row('td').eq(11).text()       #相关高管名称
                 
            ggzw = row('td').eq(12).text()       #相关高管职务
            ygggx = row('td').eq(13).text()       #与高管关系

            rowdat = [dm,bdrq,ggmc,bdgs,bdfx,bdjj,bdsz,bdlx,zgbzb,bdhcg,xggg,ggzw,ygggx]
            rowdat = [e if e!='-' else None for e in rowdat]
            data.append(rowdat)

            ts1='高管%s[%s]%s万元' % (bdlx,bdfx,bdsz)
            ts2='%s[%s]%s:%s万元' % (ggmc,ygggx,bdfx,bdsz)

            rowdat = [dm,bdrq,ts1,ts2,tslx]
            data1.append(rowdat)


        except:
            print('处理第%d页第%d行出错！' % (pgn,i))
            sc=False    #本页处理不成功
            break
    
    if sc:
        
        dbcn.executemany('''INSERT OR REPLACE INTO DFCFGGCGBD (GPDM,BDRQ,GGMC,BDGS,BDFX,
                                           BDJJ,BDSZ,BDLX,ZGBZB,BDHCG,
                                           XGGG,GGZW,YGGGX) 
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''', data)
        
        dbcn.executemany('''INSERT OR REPLACE INTO DFCF (GPDM,RQ,TS1,TS2,TSLX) 
                            VALUES (?,?,?,?,?)''', data1)

        dbcn.commit()

    dbcn.close()
            
    return sc
    
##########################################################################
#提取大股东持股变动
##########################################################################
def readdgdcgbd(dm,html,pgn):
    '''
    根据002376新北洋公告情况，会出现大股东每天频繁交易的情况，但公告是集中发布的，所以不能建唯一性索引
    
    股票代码，变动大股东名称，变动方向，变动股数，总股本占比，
    流通股占比，变动后持股总数，变动后持股总股本占比，变动后持股流通股，变动后持股流通股流通股占比，
    变动开始日期,变动截止日期，公告日期，
    CREATE TABLE [DFCFDGDCGBD](
      [GPDM] TEXT NOT NULL, 
      [DGDMC] TEXT NOT NULL, 
      [BDFX] TEXT NOT NULL, 
      [BDGS] REAL NOT NULL, 
      [ZGBZB] REAL, 
      [LTGZB] REAL, 
      [BDHCG] REAL, 
      [BDHCGZGBZB] REAL, 
      [BDHCGLTG] TEXT, 
      [BDHCGLTGZB] REAL, 
      [BDKSRQ] TEXT, 
      [BDJZRQ] TEXT, 
      [GGRQ] TEXT NOT NULL);
    
    CREATE UNIQUE INDEX [GPDM_GGMC_BDKSRQ_BDJZRQ_BDGGRQ_DFCFDGDCGBD]
    ON [DFCFDGDCGBD](
      [GPDM], 
      [DGDMC], 
      [BDKSRQ], 
      [BDJZRQ], 
      [GGRQ]);

    '''


    if '没有相关数据' in html:
        print('没有相关数据,退出')
        return True
    
    
    dm = lgpdm(dm)       
    dbfn=getdrive()+'\\hyb\\STOCKDSTX.db'
    dbcn = sqlite3.connect(dbfn)
    #大股东交易数据无法建唯一索引，避免重复只能删除
    sql = 'DELETE FROM DFCFDGDCGBD WHERE GPDM="%s";' % dm
    dbcn.execute(sql)
    dbcn.commit()
    
    html=pq(html)

    html.find("script").remove()    # 清理 <script>...</script>
    html.find("style").remove()     # 清理 <style>...</style>

    rows=html('tr')
    '''
    Python中的jquery PyQuery库使用小结
    https://www.jb51.net/article/50069.htm        

    '''
    tslx='1'
    #遍历行
    data = []
    data1 = []
    sc=True     #本页处理成功

    for i in range(len(rows)):

        try:

            row=pq(rows.eq(i))


            dgdmc = pq(row('td').eq(5))('a').attr('title')          #大股东名称
            bdfx = row('td').eq(6).text()                           #变动方向
            bdgs = row('td').eq(7).text()                           #变动股数(万股)
            zgbzb = row('td').eq(8).text()                          #变动股数总股本占比 

            ltgzb = row('td').eq(9).text()                          #变动股数流通股占比 
            bdhcg = row('td').eq(10).text()                         #变动后持股总数 (万股)
            bdhcgzgbzb = row('td').eq(11).text()                    #变动后持股总股本占比
            bdhcgltg = row('td').eq(12).text()                      #变动后持股流通股(万股) 
            bdhcgltgzb = row('td').eq(13).text()                    #变动后持股流通股占比

            bdksrq = pq(row('td').eq(14))('span').attr('title') #变动起始日期
            bdksrq = bdksrq.replace('/','-')
            bdjzrq = pq(row('td').eq(15))('span').attr('title') #变动截止日期
            bdjzrq = bdjzrq.replace('/','-')
            ggrq = pq(row('td').eq(16))('span').attr('title')   #公告日期
            ggrq = ggrq.replace('/','-')

            if ggrq!='-':
                rowdat = [dm,dgdmc,bdfx,bdgs,zgbzb,ltgzb,bdhcg,bdhcgzgbzb,bdhcgltg,bdhcgltgzb,bdksrq,bdjzrq,ggrq]
                data.append(rowdat)

            ts1='大股东(今起至%s)%s%s万股[%s公告]' % (bdjzrq,bdfx,bdgs,ggrq)
            ts2='%s[%s]%s万股,占流通比例%s' % (dgdmc,bdfx,bdgs,ltgzb)
            if bdksrq!='-':
                rowdat = [dm,bdksrq,ts1,ts2,tslx]
                data1.append(rowdat)
#            if bdjzrq!='-':
#                rowdat = [dm,bdjzrq,ts1,ts2,tslx]
#                data1.append(rowdat)
#            if ggrq!='-':
#                rowdat = [dm,ggrq,ts1,ts2,tslx]
#                data1.append(rowdat)


        except:
            print('处理第%d页第%d行出错！' % (pgn,i))

            f=open(r"d:\selestock\log.txt",'a')  
            traceback.print_exc(file=f)  
            f.flush()  
            f.close()

            sc=False    #本页处理不成功
            break
    
    if sc:
        
        dbcn.executemany('''INSERT OR REPLACE INTO DFCFDGDCGBD (GPDM,DGDMC,BDFX,BDGS,ZGBZB,
                                           LTGZB,BDHCG,BDHCGZGBZB,BDHCGLTG,BDHCGLTGZB,
                                           BDKSRQ,BDJZRQ,GGRQ) 
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''', data)
        
        dbcn.executemany('''INSERT OR REPLACE INTO DFCF (GPDM,RQ,TS1,TS2,TSLX) 
                            VALUES (?,?,?,?,?)''', data1)

        dbcn.commit()

    dbcn.close()
            
    return sc
    
##########################################################################
#股数换算成亿
##########################################################################
def gshs(s):
    if s!='-':
        return round(wyzh(s)/100000000,4)
    else:
        return None
    
##########################################################################
#提取限售解禁
##########################################################################
def readxsjj(dm,html,pgn):
    '''
    
    股票代码，解禁日期，解禁股东个数，解禁股数，实际解禁股数，
    未解禁股数，实际解禁市值，总股本占比，流通股占比，限售股类型
    
    CREATE TABLE [DFCFXSJJ](
      [GPDM] TEXT NOT NULL, 
      [JJRQ] TEXT NOT NULL, 
      [JJGDS] INTEGER NOT NULL, 
      [JJGS] REAL NOT NULL, 
      [SJJJGS] REAL, 
      [WJJGS] REAL, 
      [SJJJSZ] REAL, 
      [ZGBZB] REAL, 
      [LTGZB] REAL, 
      [XSGLX] TEXT);
    
    CREATE UNIQUE INDEX [GPDM_JJRQ_DFCFXSJJ]
    ON [DFCFXSJJ](
      [GPDM], 
      [JJRQ]);

    '''

    if '没有相关数据' in html:
        print('没有相关数据,退出')
        return True

    dbfn=getdrive()+'\\hyb\\STOCKDSTX.db'
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
    tslx='0'
    data = []
    data1 = []
    sc=True     #本页处理成功
    dm = lgpdm(dm)       
    for i in range(len(rows)):

        try:

            row=pq(rows.eq(i))

            jjrq = row('td').eq(2).text()                               #解禁日期
            jjgds = int(row('td').eq(3).text())                         #解禁股东个数
            jjgs = gshs(row('td').eq(4).text())                         #解禁股数(亿股)
            sjjjgs = gshs(row('td').eq(5).text())                       #实际解禁股数(亿股)

            wjjgs = gshs(row('td').eq(6).text())                        #未解禁股数(亿股)

            sjjjsz = gshs(row('td').eq(7).text())                       #实际解禁市值(亿元)
            zgbzb = row('td').eq(8).text()                              #实际解禁总股本占比
            zgbzb = None if zgbzb=='-' else float(zgbzb)                #实际解禁总股本占比

            ltgzb = row('td').eq(9).text()                              #实际解禁流通股占比
            ltgzb = None if ltgzb=='-' else float(ltgzb)                #实际解禁总股本占比

            xsglx = row('td').eq(11).text()                              #实际解禁总股本占比

            
            rowdat=[dm,jjrq,jjgds,jjgs,sjjjgs,wjjgs,sjjjsz,zgbzb,ltgzb,xsglx]

            data.append(rowdat)
            '''
            Python之%s%d%f
            https://blog.csdn.net/qq_37482544/article/details/63720726
            '''

            ts1='限售解禁%d个股东市值%.3f亿元' % (jjgds,sjjjsz)
            ts2='限售解禁%d个股东%.3f亿股,市值%.3f亿元,占流通比例%.4f%%,占总股本%.4f%%' % (jjgds,sjjjgs,sjjjsz,ltgzb,zgbzb)

            rowdat = [dm,jjrq,ts1,ts2,tslx]
            data1.append(rowdat)


        except:
            print('处理第%d页第%d行出错！' % (pgn,i))
            sc=False    #本页处理不成功
            break
    
    if sc:
        
        dbcn.executemany('''INSERT OR REPLACE INTO DFCFXSJJ (GPDM,JJRQ,JJGDS,JJGS,SJJJGS,
                         WJJGS,SJJJSZ,ZGBZB,LTGZB,XSGLX) 
                            VALUES (?,?,?,?,?,?,?,?,?,?)''', data)
        
        dbcn.executemany('''INSERT OR REPLACE INTO DFCF (GPDM,RQ,TS1,TS2,TSLX) 
                            VALUES (?,?,?,?,?)''', data1)

        dbcn.commit()

    dbcn.close()
            
    return sc
    

##########################################################################
#提取股东户数
##########################################################################
def readgdhs(dm,html,pgn):
    '''
    股票代码，统计截止日期，股东户数，增减比例，户均持股数，公告日期
    CREATE TABLE [DFCFGDHS](
      [GPDM] TEXT NOT NULL, 
      [RQ] TEXT NOT NULL, 
      [GDHS] INTEGER NOT NULL, 
      [ZJBL] REAL,
      [HJCGS] REAL,
      [GGRQ] TEXT);
    
    CREATE UNIQUE INDEX [GPDM_RQ_DFCFGDHS]
    ON [DFCFGDHS](
      [GPDM], 
      [RQ]);

    '''

    if '没有相关数据' in html:
        print('没有相关数据,退出')
        return True

    dbfn=getdrive()+'\\hyb\\STOCKDSTX.db'
    dbcn = sqlite3.connect(dbfn)
    
    html=pq(html)

    html.find("script").remove()    # 清理 <script>...</script>
    html.find("style").remove()     # 清理 <style>...</style>

    rows=html('tr')
    '''
    Python中的jquery PyQuery库使用小结
    https://www.jb51.net/article/50069.htm        

    '''
    tslx='4'
    #遍历行
    data = []
    data1 = []
    sc=True     #本页处理成功
    dm = lgpdm(dm)       
    for i in range(len(rows)):

        try:

            row=pq(rows('tr').eq(i))

            rq = row('td').eq(0).text()                         #统计截止日期
            rq = rq.replace('/','-')
            gdhs = row('td').eq(2).text()                       #股东户数
            zjbl = str2num(row('td').eq(5).text())              #增减比例
            hjcgs = str2num(row('td').eq(5).text())                      #户均持股数(万股)

            ggrq = pq(row('td').eq(12))('span').attr('title')
            ggrq=ggrq.replace('/','-')
 
            rowdat = [dm,rq,gdhs,zjbl,hjcgs,ggrq]
            data.append(rowdat)

            ts1='股东户数%s%.2f%%' % (('增加' if zjbl>0 else '减少'),zjbl)
            ts2='股东户数%s%.2f%%,户均持股%.2f万股' % (('增加' if zjbl>0 else '减少'),zjbl,hjcgs)

            rowdat = [dm,rq,ts1,ts2,tslx]
            data1.append(rowdat)

        except:
            print('处理第%d页第%d行出错！' % (pgn,i))
            sc=False    #本页处理不成功
            break
    
    if len(data)>0 and sc:
        
        dbcn.executemany('INSERT OR REPLACE INTO DFCFGDHS (GPDM,RQ,GDHS,ZJBL,HJCGS,GGRQ) VALUES (?,?,?,?,?,?)', data)
        
        dbcn.executemany('''INSERT OR REPLACE INTO DFCF (GPDM,RQ,TS1,TS2,TSLX) 
                            VALUES (?,?,?,?,?)''', data1)

        dbcn.commit()

    dbcn.close()
            
    return sc

##########################################################################
#提取股票回购
##########################################################################
def readgphg(dm,html,pgn):
    '''
    股票代码，回购数量区间，流通股占比，总股本占比，回购金额，回购起始日期，实施进度，公告日期
    CREATE TABLE [DFCFGPHG](
      [GPDM] TEXT NOT NULL, 
      [GGRQ] TEXT NOT NULL, 
      [HGSL] TEXT NOT NULL, 
      [LTGZB] TEXT,
      [ZGBZB] TEXT,
      [HGJE] TEXT,
      [QSRQ] TEXT,
      [SSJD] TEXT);
    
    CREATE UNIQUE INDEX [GPDM_RQ_DFCFGPHG]
    ON [DFCFGPHG](
      [GGRQ]);

    '''
    if '没有相关数据' in html:
        print('没有相关数据,退出')
        return True
    
    dbfn=getdrive()+'\\hyb\\STOCKDSTX.db'
    dbcn = sqlite3.connect(dbfn)
    
    html=pq(html)

    html.find("script").remove()    # 清理 <script>...</script>
    html.find("style").remove()     # 清理 <style>...</style>

    rows=html('tr')
    
    '''
    Python中的jquery PyQuery库使用小结
    https://www.jb51.net/article/50069.htm        

    '''
    tslx='5'
    #遍历行
    data = []
    data1 = []
    sc=True     #本页处理成功
    dm = lgpdm(dm)       
    for i in range(len(rows)):

        try:

            row=pq(rows('tr').eq(i))

            ggrq = row('td').eq(13).text()                          #公告日期

            hgsl = row('td').eq(7).text()                           #回购数量区间
            ltgzb = row('td').eq(8).text()            #回购数量占公告前一日流通股份比例(%)
            zgbzb = row('td').eq(9).text()            #回购数量占公告前一日总股本比例(%)
            hgje = row('td').eq(10).text()                           #回购金额
            qsrq = row('td').eq(11).text()                          #起始日期
            ssjd = row('td').eq(12).text()                          #实施进度
 
            rowdat = [dm,ggrq,hgsl,ltgzb,zgbzb,hgje,qsrq,ssjd]
            data.append(rowdat)

            if hgje=='1.00':
                ts1='并购业绩对赌回购注销'
                ts2='并购业绩对赌回购注销%s股' % hgsl
                rowdat = [dm,ggrq,ts1,ts2,tslx]
                data1.append(rowdat)
            elif qsrq != '-':
                ts1='回购预案公布'
                ts2='拟回购%s股,共耗资%s元,占总股本%s%%' % (hgsl,hgje,zgbzb)
                rowdat = [dm,ggrq,ts1,ts2,tslx]
                data1.append(rowdat)

                ts1='回购开始实施'
                ts2='拟回购%s股,共耗资%s元,占总股本%s%%' % (hgsl,hgje,zgbzb)
                rowdat = [dm,qsrq,ts1,ts2,tslx]
                data1.append(rowdat)
            else:
                ts1='回购预案公布'
                ts2='拟回购%s股,共耗资%s元,占总股本%s%%' % (hgsl,hgje,zgbzb)
                rowdat = [dm,ggrq,ts1,ts2,tslx]
                data1.append(rowdat)
                
                
        except:
            print('处理第%d页第%d行出错！' % (pgn,i))
            sc=False    #本页处理不成功
            break
    
    if len(data)>0 and sc:
        
        dbcn.executemany('''INSERT OR REPLACE INTO DFCFGPHG (GPDM,GGRQ,HGSL,LTGZB,ZGBZB,HGJE,QSRQ,SSJD) 
                            VALUES (?,?,?,?,?,?,?,?)''', data)
        
        dbcn.executemany('''INSERT OR REPLACE INTO DFCF (GPDM,RQ,TS1,TS2,TSLX) 
                            VALUES (?,?,?,?,?)''', data1)

        dbcn.commit()

    dbcn.close()
            
    return sc


##########################################################################
#提取公告信息
##########################################################################
def readggxx(dm,html,pgn):
    '''
    股票代码，公告发布日期，公告标题，公告网址，公告类型
    CREATE TABLE [DFCFGGXX](
      [GPDM] TEXT NOT NULL, 
      [RQ] TEXT NOT NULL, 
      [GGBT] TEXT NOT NULL, 
      [GGWZ] TEXT,
      [GGLX] TEXT);
    
    CREATE UNIQUE INDEX [GPDM_RQ_GGBT_DFCFGGXX]
    ON [DFCFGGXX](
      [GPDM], 
      [RQ],
      [GGBT]);
            
    提取下列公告类型：
        半年度报告全文
        分配方案实施
        分配预案
        股东/实际控制人股份减持
        股东/实际控制人股份增持
        股份质押、冻结
        股权转让
        借贷
        年度报告全文
        三季度报告全文
        收购出售资产/股权
        限售股份上市流通
        业绩快报
        业绩预告
        一季度报告全文
        增发获准公告
        增发预案
        重大合同

    '''
    lxlb='''
        半年度报告全文
        分配方案实施
        分配预案
        股东/实际控制人股份减持
        股东/实际控制人股份增持
        股份质押、冻结
        股权转让
        借贷
        年度报告全文
        三季度报告全文
        收购出售资产/股权
        限售股份上市流通
        业绩快报
        业绩预告
        一季度报告全文
        增发获准公告
        增发预案
        重大合同
        诉讼仲裁
    '''
    if '没有相关数据' in html:
        print('没有相关数据,退出')
        return True
    
    url0='http://data.eastmoney.com'

    dbfn=getdrive()+'\\hyb\\STOCKDSTX.db'
    dbcn = sqlite3.connect(dbfn)
    
    html=pq(html)

    html.find("script").remove()    # 清理 <script>...</script>
    html.find("style").remove()     # 清理 <style>...</style>

    rows=html('tr')
    '''
    Python中的jquery PyQuery库使用小结
    https://www.jb51.net/article/50069.htm        

    '''
    tslx='6'
    #遍历行
    data = []
    data1 = []
    sc=True     #本页处理成功
    dm = lgpdm(dm)       
    for i in range(len(rows)):

        try:

            row=pq(rows('tr').eq(i))

            rq = row('td').eq(2).text()                         #公告发布日期

            ggbt = pq(row('td').eq(0))('a').attr('title')       #公告标题
            ggwz = url0+pq(row('td').eq(0))('a').attr('href')   #公告网址

            gglx = row('td').eq(1).text()                       #公告类型

            if gglx in lxlb:
                
                rowdat = [dm,rq,ggbt,ggwz,gglx]
                data.append(rowdat)
    
                ts1='公告[%s]' % gglx
                ts2=ggbt
    
                rowdat = [dm,rq,ts1,ts2,tslx]
                data1.append(rowdat)

        except:
            print('处理第%d页第%d行出错！' % (pgn,i))
            sc=False    #本页处理不成功
            break
    
    if len(data)>0 and sc:
        
        dbcn.executemany('''INSERT OR REPLACE INTO DFCFGGXX (GPDM,RQ,GGBT,GGWZ,GGLX) 
                            VALUES (?,?,?,?,?)''', data)
        
        dbcn.executemany('''INSERT OR REPLACE INTO DFCF (GPDM,RQ,TS1,TS2,TSLX) 
                            VALUES (?,?,?,?,?)''', data1)

        dbcn.commit()

    dbcn.close()
            
    return sc


##########################################################################
#提取业绩报表
##########################################################################
def readeastmoney(url,lb,lbmc):


    fireFoxOptions = webdriver.FirefoxOptions()
    fireFoxOptions.set_headless()
    browser = webdriver.Firefox(firefox_options=fireFoxOptions)

#    browser = webdriver.Firefox()


    browser.get(url)
        
    try:
        '''
        EC.presence_of_element_located()传递的参数是tuple元组
        '''
        elem=WebDriverWait(browser, 10).until(
            EC.presence_of_element_located((By.XPATH, "//a[text()='下一页']//preceding-sibling::a[1]")))
        pgs=int(elem.text)
    except:
        pgs=1

#    html = browser.find_element_by_xpath("//*").get_attribute("outerHTML")
#    html = pq(html)
#    html.find("script").remove()    # 清理 <script>...</script>
#    html.find("style").remove()     # 清理 <style>...</style>
    pgn=1   
    errcs=0
    while pgn<=pgs:

        print("正在处理【%s】第%d/%d页，请等待。" % (lbmc,pgn,pgs))
        if pgn>1:
            try :   
                if lb in ('ggyb',):
                    elem = WebDriverWait(browser, 20).until(
                        EC.presence_of_element_located((By.ID, "gopage")))                    
                else:
                    elem = WebDriverWait(browser, 20).until(
                        EC.presence_of_element_located((By.ID, "PageContgopage")))                    
                    
                elem.clear()
                #输入页面
                elem.send_keys(pgn)
                elem = browser.find_element_by_class_name("btn_link")     
                #点击Go
                elem.click()


                #定位到表体,可能dt_1已调入，但表体数据没有完成调入，只有一行“数据加载中...”
                if lb in ('xsjj',):
                    tbl = WebDriverWait(browser, 10).until(
                            EC.presence_of_element_located((By.ID, "td_1")))
                else:
                    tbl = WebDriverWait(browser, 10).until(
                            EC.presence_of_element_located((By.ID, "dt_1")))
                    
                if lb in ('ggyb',):
                    WebDriverWait(tbl, 10).until(
                            EC.presence_of_element_located((By.CLASS_NAME, "change")))
                elif lb in ('ggxx',):
                    WebDriverWait(tbl, 10).until(
                            EC.presence_of_element_located((By.CLASS_NAME, "overflow")))
                elif lb in ('xsjj','dgdcgbd','gphg'):
                    WebDriverWait(tbl, 10).until(
                            EC.presence_of_element_located((By.CLASS_NAME, "col")))
                elif lb in ('ggcgbd',):
                    WebDriverWait(tbl, 10).until(
                            EC.presence_of_element_located((By.CLASS_NAME, "txt")))
                else:
                     WebDriverWait(tbl, 10).until(
                            EC.presence_of_element_located((By.TAG_NAME, "span")))
                   
                #重新定位是考虑加载数据前后可能内容不一致，保证数据的完整   
                if lb in ('xsjj',):
                    tbl = browser.find_element_by_id('td_1')
                else:
                    tbl = browser.find_element_by_id('dt_1')
                    
            except :
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
                #定位到表体,可能dt_1已调入，但表体数据没有完成调入，只有一行“数据加载中...”
                if lb in ('xsjj',):
                    tbl = WebDriverWait(browser, 10).until(
                            EC.presence_of_element_located((By.ID, "td_1")))
                else:
                    tbl = WebDriverWait(browser, 10).until(
                            EC.presence_of_element_located((By.ID, "dt_1")))
                    
                html=tbl.get_attribute('innerHTML')
                if '没有相关数据' not in html:
                    if lb in ('ggyb',):
                        WebDriverWait(tbl, 10).until(
                                EC.presence_of_element_located((By.CLASS_NAME, "change")))
                    elif lb in ('ggxx',):
                        WebDriverWait(tbl, 10).until(
                                EC.presence_of_element_located((By.CLASS_NAME, "overflow")))
                    elif lb in ('xsjj','dgdcgbd','gphg'):
                        WebDriverWait(tbl, 10).until(
                                EC.presence_of_element_located((By.CLASS_NAME, "col")))
                    elif lb in ('ggcgbd',):
                        WebDriverWait(tbl, 10).until(
                                EC.presence_of_element_located((By.CLASS_NAME, "txt")))
                    else:
                         WebDriverWait(tbl, 10).until(
                                EC.presence_of_element_located((By.TAG_NAME, "span")))
                   
                #重新定位是考虑加载数据前后可能内容不一致，保证数据的完整   
                if lb in ('xsjj',):
                    tbl = browser.find_element_by_id('td_1')
                else:
                    tbl = browser.find_element_by_id('dt_1')
            except :
                browser.quit()
                f=open(r"d:\selestock\log.txt",'a')  
                traceback.print_exc(file=f)  
                f.flush()  
                f.close()

                print("1出错退出")
                sys.exit()

        if lb in ('ggyb',):
            html=tbl.get_attribute('innerHTML')
        else:
            tbody = tbl.find_element_by_tag_name('tbody')
            html=tbody.get_attribute('innerHTML')

        if lb=='ggyb':
            sc=readggyb(dm,html,pgn)
        elif lb=='ggcgbd' :
            sc=readggcgbd(dm,html,pgn)
        elif lb=='dgdcgbd' :
            sc=readdgdcgbd(dm,html,pgn)
        elif lb=='xsjj' :
            sc=readxsjj(dm,html,pgn)
        elif lb=='gdhs' :
            sc=readgdhs(dm,html,pgn)
        elif lb=='gphg' :
            sc=readgphg(dm,html,pgn)
        elif lb=='ggxx' :
            sc=readggxx(dm,html,pgn)
        else :
            print('调用类别错误！')
            sys.exit()            
            
        if sc:
            pgn += 1
            
        browser.get(url)
                
    
    browser.quit()    

    return
    
##########################################################################
#清空表
##########################################################################
def emptytbl(dm):
    dbfn=getdrive()+'\\hyb\\STOCKDSTX.db'
    dbcn = sqlite3.connect(dbfn)
    for tbl in ('DFCF','DFCFXSJJ','DFCFYB','DFCFDGDCGBD','DFCFGGCGBD','DFCFGDHS','DFCFGPHG','DFCFGGXX'):
    #大股东交易数据无法建唯一索引，避免重复只能删除
        sql = 'DELETE FROM %s WHERE GPDM="%s";' % (tbl,lgpdm(dm))
        dbcn.execute(sql)
    dbcn.commit()
    return

##########################################################################
#生成大事提醒表
##########################################################################
def gendstx(dm):
    dbfn=getdrive()+'\\hyb\\STOCKDSTX.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    
    lbgl = 'and tslx in ("0","1","2")'
#    lbgl = 'and tslx in ("0","1","2")'
    lbgl=''
    
    sql = 'select distinct rq,ts1,ts2,tslx from dfcf where gpdm="%s" %s order by rq,tslx;' % (lgpdm(dm),lbgl)
    
    curs.execute(sql)
    
    data = curs.fetchall()
    
    
    cols = ['rq','ts1','ts2','tslx']
    
    df = pd.DataFrame(data,columns=cols)
    df['rq']=pd.to_datetime(df['rq'])
    df = df.set_index('rq',drop=False)
    
    df1 = df.drop_duplicates(['rq','ts1'],keep='first')
        
    rqlst=list(set(df1.index))
    rqtx=[]
    rqlst.sort()
    for rq in rqlst:
        ts=df1.loc[rq]['ts1']
        if not isinstance(ts,str):
            for i in range(len(ts)):
                if i==0:
                    tsxx=ts.iloc[i]
                else:
                    tsxx=tsxx+'、'+ts.iloc[i]
            ts=tsxx
            
        rqtx.append([rq,ts])    
    
    df1=pd.DataFrame(rqtx,columns=['rq','ts1'])
    df1=df1.set_index('rq',drop=False)

    fn1 = r'd:\selestock\tsxx%s.xlsx' % sgpdm(dm)

    writer=pd.ExcelWriter(fn1,engine='xlsxwriter')
    df1.to_excel(writer, sheet_name='每日大事提醒',index=False)  
    writer.save()
    
    fn = r'd:\selestock\dstx%s.xlsx' % sgpdm(dm)
    writer=pd.ExcelWriter(fn,engine='xlsxwriter')

    workbook = writer.book
    
    cell_format1 = workbook.add_format({#一种方法可以直接在字典里 设置属性
                                        'font_size':  9,   #字体大小
                                        'align':    'center',
                                        'valign':   'vcenter',
                                        'num_format': 'yyyy-mm-dd'
                                        })

    cell_format3 = workbook.add_format({#一种方法可以直接在字典里 设置属性
                                        'font_size':  9,   #字体大小
                                        'align':    'left',
                                        'valign':   'vcenter',
                                        })
    cell_format4 = workbook.add_format({
                                        'border':1,       #单元格边框宽度
                                        })
    cell_format5 = workbook.add_format({'bg_color': '#FFC7CE',
                                       'font_color': '#9C0006'
                                       })

    df.to_excel(writer, sheet_name='大事提醒明细',index=False)   

    worksheet = writer.sheets['大事提醒明细']
    shtdic=worksheet.__dict__
    rows=shtdic['dim_rowmax']
    cols=shtdic['dim_colmax']
    worksheet.set_column('A:A', 20,cell_format1)
    worksheet.set_column('B:B', 100,cell_format3)
    worksheet.set_column('C:C', 120,cell_format3)
    worksheet.set_column('D:D', 5,cell_format3)

    worksheet.conditional_format(0,0,rows,cols, {'type': 'cell',
                                     'criteria': '!=',
                                     'value': 0,
                                     'format': cell_format4})


    worksheet.conditional_format(0,0,rows,cols, {'type':     'formula',
                                    'criteria': '=mod(row(),2)=0',
                                    'format':   cell_format5})

    df1.to_excel(writer, sheet_name='每日大事提醒',index=False)  


    worksheet = writer.sheets['每日大事提醒']
    shtdic=worksheet.__dict__
    rows=shtdic['dim_rowmax']
    cols=shtdic['dim_colmax']
    worksheet.set_column('A:A', 20,cell_format1)
    worksheet.set_column('B:B', 100,cell_format3)

    worksheet.conditional_format(0,0,rows,cols, {'type': 'cell',
                                     'criteria': '!=',
                                     'value': 0,
                                     'format': cell_format4})


    worksheet.conditional_format(0,0,rows,cols, {'type':     'formula',
                                    'criteria': '=mod(row(),2)=0',
                                    'format':   cell_format5})


    writer.save()
    
    return
 
    
##########################################################################
#从东方财富网抓取数据，生成大事提醒
##########################################################################
def getdstx(dm):
    
    
    items=[['ggyb',"http://data.eastmoney.com/report/%s.html" % dm,'个股研报'],
           ['ggcgbd',"http://data.eastmoney.com/executive/%s.html" % dm,'高管持股变动'],
           ['dgdcgbd',"http://data.eastmoney.com/executive/gdzjc/%s.html" % dm,'大股东持股变动'],
           ['xsjj','http://data.eastmoney.com/dxf/q/%s.html' % dm,'限售解禁'],
           ['gdhs','http://data.eastmoney.com/gdhs/detail/%s.html' % dm,'股东户数'],
           ['gphg','http://data.eastmoney.com/gphg/%s.html' % dm,'股票回购'],
           ['ggxx','http://data.eastmoney.com/notices/stock/%s.html' % dm,'公告信息']]
    
#    ,
#           ['ggrl','http://data.eastmoney.com/Stockcalendar/%s.html' % dm,'个股日历']]
    
#    lb,url,lbmc = items[5]
    for lb,url,lbmc in items:
        readeastmoney(url,lb,lbmc)

    return


def tmp():
    lb='ggxx'
    lbmc='公告信息'
    url='http://data.eastmoney.com/notices/stock/%s.html' % dm


#    fireFoxOptions = webdriver.FirefoxOptions()
#    fireFoxOptions.set_headless()
#    browser = webdriver.Firefox(firefox_options=fireFoxOptions)

    browser = webdriver.Firefox()


    browser.get(url)
    
#    elem=browser.find_element_by_xpath('//div[@id=\"tabMenu\"]/ul/li[@data-ul=\"zccz_ul\"]')
#    elem=browser.find_element_by_xpath('//li[@data-ul=\"zccz_ul\"]')
        
    elem=WebDriverWait(browser, 10).until(
        EC.presence_of_element_located((By.XPATH, '//li[@data-ul=\"zdsx_ul\"]')))
    elem.click()
    
    try:
        '''
        EC.presence_of_element_located()传递的参数是tuple元组
        '''
        elem=WebDriverWait(browser, 10).until(
            EC.presence_of_element_located((By.XPATH, "//a[text()='下一页']//preceding-sibling::a[1]")))
        pgs=int(elem.text)
    except:
        pgs=1

    return
   
if __name__ == "__main__": 
    
    '''
    由于股票质押数据只保存了最新的情况，这里就不采集了
    ['gpzy','http://data.eastmoney.com/gpzy/detail/%s.html' % dm,'股票质押'],
    股东大会列表能提供的信息也有限
    ['gddh','http://data.eastmoney.com/gddh/list/%s.html' % dm,'股东大会']
    '''
    print('%s Running' % sys.argv[0])
    now1 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now1)

#    today=datetime.datetime.now().strftime('%Y-%m-%d')
#    config = iniconfig()
#    lastdate = readkey(config,'lastdate','2018-07-01')

    dm='002458'

    
    emptytbl(dm)

    getdstx(dm)

    gendstx(dm)
#    print('\n')

#    config['lastdate'] = today
#    config.write()
    
#    now2 = datetime.datetime.now().strftime('%H:%M:%S')
#    print('开始运行时间：%s' % now1)
#    print('结束运行时间：%s' % now2)
#
#
