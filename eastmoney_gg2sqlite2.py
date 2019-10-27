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
def wyzh(s):
    s=s.replace('万','*10000').replace('亿','*100000000')
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


            dm = lgpdm(dm)       
            rowdat = [dm,rq,pjlb,pjbd,pjjg,ybbt,ybwz]
            rowdat = [e if e!='-' else np.nan for e in rowdat]
            data.append(rowdat)

            ts1='%s[%s,%s]' % (pjlb,pjjg,pjbd)
            ts2='%s[%s]%s' % (pjlb,pjjg,ybbt)

            rowdat = [dm,rq,ts1,ts2,'0']
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


            dm = lgpdm(dm)       
            rowdat = [dm,bdrq,ggmc,bdgs,bdfx,bdjj,bdsz,bdlx,zgbzb,bdhcg,xggg,ggzw,ygggx]
            rowdat = [e if e!='-' else None for e in rowdat]
            data.append(rowdat)

            ts1='高管%s[%s]' % (bdfx,bdlx)
            ts2='%s[%s]%s:%s万元' % (ggmc,ygggx,bdfx,bdsz)

            rowdat = [dm,bdrq,ts1,ts2,'0']
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
    data = []
    data1 = []
    sc=True     #本页处理成功
    dm = lgpdm(dm)       
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


            rowdat = [dm,dgdmc,bdfx,bdgs,zgbzb,ltgzb,bdhcg,bdhcgzgbzb,bdhcgltg,bdhcgltgzb,bdksrq,bdjzrq,ggrq]
            data.append(rowdat)

            ts1='大股东%s[%s公告]' % (bdfx,ggrq)
            ts2='%s[%s]%s万股,占流通比例%s' % (dgdmc,bdfx,bdgs,ltgzb)

            rowdat = [dm,bdksrq,ts1,ts2,'0']
            data1.append(rowdat)
            rowdat = [dm,bdjzrq,ts1,ts2,'0']
            data1.append(rowdat)
            rowdat = [dm,ggrq,ts1,ts2,'0']
            data1.append(rowdat)


        except:
            print('处理第%d页第%d行出错！' % (pgn,i))
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
      
      [WJJJGS] REAL, 
      [SJJJSZ] REAL, 
      [ZGBZB] REAL, 
      [LTGZB] REAL, 
      [XSGLX] TEXT);
    
    CREATE UNIQUE INDEX [GPDM_JJRQ_DFCFXSJJ]
    ON [DFCFXSJJ](
      [GPDM], 
      [JJRQ]);

    '''
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
    data = []
    data1 = []
    sc=True     #本页处理成功
    dm = lgpdm(dm)       
    for i in range(len(rows)):

        try:

            row=pq(rows.eq(i))

            jjrq = row('td').eq(2).text()                               #解禁日期
            jjgds = int(row('td').eq(3).text())                         #解禁股东个数
            jjgs = round(wyzh(row('td').eq(4).text())/100000000,4)      #解禁股数(亿股)
            sjjjgs = round(wyzh(row('td').eq(5).text())/100000000,4)    #实际解禁股数(亿股)
            wjjgs = round(wyzh(row('td').eq(6).text())/100000000,4)     #未解禁股数(亿股)
            sjjjsz = round(wyzh(row('td').eq(7).text())/100000000,4)    #实际解禁市值(亿元)
            zgbzb = row('td').eq(8).text()                              #实际解禁总股本占比
            zgbzb = None if zgbzb=='-' else float(zgbzb)                #实际解禁总股本占比

            ltgzb = row('td').eq(9).text()                              #实际解禁流通股占比
            ltgzb = None if ltgzb=='-' else float(ltgzb)                #实际解禁总股本占比

            xsglx = row('td').eq(11).text()                              #实际解禁总股本占比

            
            rowdat=[dm,jjrq,jjgds,jjgs,sjjjgs,wjjgs,sjjjsz,zgbzb,ltgzb,xsglx]

            data.append(rowdat)

            ts1='限售解禁%d亿元' % sjjjsz
            ts2='限售解禁%d亿股,市值%d亿元,占流通比例%d%%,占总股本%d%%' % (sjjjgs,sjjjsz,ltgzb,zgbzb)

            rowdat = [dm,jjrq,ts1,ts2,'0']
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

    while pgn<=pgs:

        print("正在处理【%s】第%d/%d页，请等待。" % (lbmc,pgn,pgs))
        if pgn>1:
            try :   
                if lb in ('ggyb',):
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
                tbl = WebDriverWait(browser, 10).until(
                        EC.presence_of_element_located((By.ID, "dt_1")))
                if lb in ('ggyb',):
                    WebDriverWait(tbl, 10).until(
                            EC.presence_of_element_located((By.CLASS_NAME, "change")))
                else:
                     WebDriverWait(tbl, 10).until(
                            EC.presence_of_element_located((By.CLASS_NAME, "odd")))
                   
                #重新定位是考虑加载数据前后可能内容不一致，保证数据的完整   
                tbl = browser.find_element_by_id('dt_1')
            except :
                browser.quit()
                print("0出错退出")
                sys.exit()
        else:
            try:
                tbl = WebDriverWait(browser, 10).until(
                        EC.presence_of_element_located((By.ID, "dt_1")))
                if lb in ('ggyb',):
                    WebDriverWait(tbl, 10).until(
                            EC.presence_of_element_located((By.CLASS_NAME, "change")))
                else:
                     WebDriverWait(tbl, 10).until(
                            EC.presence_of_element_located((By.CLASS_NAME, "odd")))
                #重新定位是考虑加载数据前后可能内容不一致，保证数据的完整   
                tbl = browser.find_element_by_id('dt_1')
            except :
                browser.quit()
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
                
    
    browser.quit()    

    return
    

if __name__ == "__main__": 
    print('%s Running' % sys.argv[0])
    now1 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now1)

#    today=datetime.datetime.now().strftime('%Y-%m-%d')
#    config = iniconfig()
#    lastdate = readkey(config,'lastdate','2018-07-01')

    dm='002376'

    items=[['ggyb',"http://data.eastmoney.com/report/%s.html" % dm,'个股研报'],
           ['ggcgbd',"http://data.eastmoney.com/executive/%s.html" % dm,'高管持股变动'],
           ['dgdcgbd',"http://data.eastmoney.com/executive/gdzjc/%s.html" % dm,'大股东持股变动'],
           ['xsjj','http://data.eastmoney.com/dxf/q/%s.html' % dm,'限售解禁'],
           ['gdhs','http://data.eastmoney.com/gdhs/detail/%s.html' % dm,'股东户数'],
           ['gphg','http://data.eastmoney.com/gphg/%s.html' % dm,'股票回购'],
           ['gpzy','http://data.eastmoney.com/gpzy/detail/%s.html' % dm,'股票质押'],
           ['gddh','http://data.eastmoney.com/gddh/list/%s.html' % dm,'股东大会']]
    
#    for lb,url,lbmc in items:
    lb,url,lbmc = items[3]
    readeastmoney(url,lb,lbmc)
#    print('\n')

#    config['lastdate'] = today
#    config.write()
    
    now2 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now1)
    print('结束运行时间：%s' % now2)
