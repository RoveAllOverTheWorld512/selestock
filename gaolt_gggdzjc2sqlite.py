# -*- coding: utf-8 -*-
"""
从港澳资讯灵通V7 提取高管、股东增减持数据导入Sqlite数据库
"""
#from selenium import webdriver
#from selenium.webdriver.common.by import By
#from selenium.webdriver.support.ui import WebDriverWait
#from selenium.webdriver.support import expected_conditions as EC
from pyquery import PyQuery as pq
import datetime
import time
import sqlite3
import sys
import os
import re
import pandas as pd
import numpy as np
import struct
import socket
import selestock as my

########################################################################
#获取工作目录，程序路径
########################################################################
def samepath():
    if sys.argv[0]!='' :
        (filepath,tempfilename) = os.path.split(sys.argv[0])
        return filepath
    else :
        return os.getcwd()


########################################################################
#从港澳资讯灵通v7获取股东户数
########################################################################
def get_gdhs(gpdm):
    
    gpdm=my.sgpdm(gpdm)
    
    data=[]
    
    url = 'http://stock.romaway.com/stock/main.asp?StockCode=%s&Flag=12' % gpdm
    
    try :

#        browser.get(url)
#        html = browser.find_element_by_xpath("//*").get_attribute("outerHTML")
#        html = pq(html)

        html = pq(url,encoding="GBK")

        tbl = html('table').eq(1).text()
        aa=re.findall('【2.股东户数变化】(.*?)【3.机构持股明细】',tbl,re.S)
        
        if len(aa)==2:
            #https://zhidao.baidu.com/question/178785538.html
            #https://zhidao.baidu.com/question/1865623377349562307.html?fr=iks&word=%BF%D5%B8%F1+%5Cxa0+ISO_8859-1&ie=gbk
            #\xa0属于latin1（ISO/IEC_8859-1）中的扩展字符集字符,代表空白符nbsp(non-breaking space)。
            aa=aa[1].translate(move)
#            print('gg')       

            if not '无数据' in aa:
                aa1=re.findall('├──────┼───────┼──────┼──────┼──────┤(.*?)└──────┴───────┴──────┴──────┴──────┘',aa,re.S)
                bb=re.findall('\n(.+)',aa1[0])
                
                if len(bb)>0:
                        
                    i=0
                    while i<len(bb):  
    
                        cc=re.findall('(.*?)｜',bb[i])
                        bdrq = cc[1]      #截止日期 
                        gdhs = cc[2]    #股东户数
                        hjltg = cc[3]   #户均流通股
                        aghs = cc[4]    #A股户数
                        bdfd = cc[5]    #变动幅度(%)
                        aghs = aghs if aghs!='-' else ''
                        bdfd = bdfd if bdfd!='-' else ''
                        
                        data.append([my.lgpdm(gpdm),bdrq,gdhs,hjltg,aghs,bdfd])
                        
                        i = i+1

    except : 
        print("股东户数,出错退出")
    
    return data



########################################################################
#从港澳资讯灵通v7获取高管持股变动
########################################################################
def get_ggcgbd1(gpdm):
    
    gpdm=my.sgpdm(gpdm)
    
    data=[]
    
    url = 'http://stock.romaway.com/stock/main.asp?StockCode=%s&Flag=14' % gpdm
    
    try :

#        browser.get(url)
#        html = browser.find_element_by_xpath("//*").get_attribute("outerHTML")
#        html = pq(html)

        html = pq(url,encoding="GBK")

        tbl = html('table').eq(1).text()
        aa=re.findall('上市公司董事、监事、高级管理人员及相关人员持有本公司股份变动情况：(.*?)【2\.高管列表】',tbl,re.S)
        
        if len(aa)==1:
            #https://zhidao.baidu.com/question/178785538.html
            #https://zhidao.baidu.com/question/1865623377349562307.html?fr=iks&word=%BF%D5%B8%F1+%5Cxa0+ISO_8859-1&ie=gbk
            #\xa0属于latin1（ISO/IEC_8859-1）中的扩展字符集字符,代表空白符nbsp(non-breaking space)。
            aa=aa[0].translate(move)
#            print('gg')       

            if not '无数据' in aa:
                aa1=re.findall('├───┼────┼───┼───┼───┼───┼───┼──┼───┤(.*?)└───┴────┴───┴───┴───┴───┴───┴──┴───┘',aa,re.S)
                bb=re.findall('\n(.+)',aa1[0])
                
                if len(bb)>0:
    
                    td1=''
                    td2=''
                    td3=''
                    td4=''
                    td5=''
                    td6=''
                    td7=''
                    td8=''
                    td9=''
                    
                    i=0
                    while i<len(bb):  
    
                        cc=re.findall('(.*?)｜',bb[i])
    
                        #用日期作为新的一行开始，
                        if cc[2]=='':
    
                            td1 = td1 + cc[1]
                            td2 = td2 + cc[2]
                            td3 = td3 + cc[3]
                            td4 = td4 + cc[4]
                            td5 = td5 + cc[5]
                            td6 = td6 + cc[6]
                            td7 = td7 + cc[7]
                            td8 = td8 + cc[8]
                            td9 = td9 + cc[9]
    
                            #最后一行
                            if i>=len(bb)-1:
    
                                bdrq = td2
                                bdrq = bdrq[:4]+'-'+bdrq[4:6]+'-'+bdrq[6:]
                                bdr = td1
    
                                if td6!='-':
                                    sygf = round(my.str2float_none(td6),0)
                                else:
                                    sygf = 0
    
                                bdsl = round(my.str2float_none(td3),0)
                                if bdsl>0:
                                    bdfx='增持'
                                else:
                                    bdfx='减持'
                                    
                                data.append([my.lgpdm(gpdm),bdrq,bdr,bdsl,sygf,bdfx])
                        else:
                            #第一行
                            if i>0:
                                bdrq = td2
                                bdrq = bdrq[:4]+'-'+bdrq[4:6]+'-'+bdrq[6:]
                                bdr = td1
                                if td6!='-':
                                    sygf = round(my.str2float_none(td6),0)
                                else:
                                    sygf = 0
                                    
                                bdsl = round(my.str2float_none(td3),0)
                                if bdsl>0:
                                    bdfx='增持'
                                else:
                                    bdfx='减持'
                                    
                                data.append([my.lgpdm(gpdm),bdrq,bdr,bdsl,sygf,bdfx])
    
                            td1 =cc[1]
                            td2 =cc[2]
                            td3 =cc[3]
                            td4 =cc[4]
                            td5 =cc[5]
                            td6 =cc[6]
                            td7 =cc[7]
                            td8 =cc[8]
                            td9 =cc[9]
                        
                        i = i+1

    except : 
        print("高管持股变动,出错退出")
    
    return data

 
########################################################################
#从港澳资讯灵通v7获取股东持股变动
########################################################################
def get_gdcgbd1(gpdm):
    
    gpdm=my.sgpdm(gpdm)
    
    data=[]
    
    url = 'http://stock.romaway.com/stock/main.asp?StockCode=%s&Flag=4' % gpdm
    
    try :
                
        html = pq(url,encoding="GBK")

        tbl = html('table').eq(1).text()
        aa=re.findall('【2\.股东持股变动】(.*?)【3\.股东变化】',tbl,re.S)

        if len(aa)==2:

            aa=aa[1].translate(move)

            if not '无数据' in aa:
                aa1=re.findall('├─────┼────┼────┼────┼────┼────┼────┤(.*?)└─────┴────┴────┴────┴────┴────┴────┘',aa,re.S)
                bb=re.findall('\n(.+)',aa1[0])
    
                if len(bb)>0:
    
                    td1=''
                    td2=''
                    td3=''
                    td4=''
                    td5=''
                    td6=''
                    td7=''

                    i=0
                    while i<len(bb):  
    
                        cc=re.findall('(.*?)｜',bb[i])
    
                        #用日期作为新的一行开始，
                        #Python and和or运算符进一步说明，
                        #https://blog.csdn.net/u011973222/article/details/79921391
                        
                        if len(cc[7])==0 or cc[7][-1]!='-':
    
                            td1 = td1 + cc[1]
                            td2 = td2 + cc[2]
                            td3 = td3 + cc[3]
                            td4 = td4 + cc[4]
                            td5 = td5 + cc[5]
                            td6 = td6 + cc[6]
                            td7 = td7 + cc[7]
    
                            #最后一行
                            if i>=len(bb)-1:
                                bdrq = td7
                                bdr = td1
                                bdsl = round(my.str2float_none(td3)*10000,0)
                                sygf = round(my.str2float_none(td4)*10000,0)
                                if my.str2float_none(td2)<my.str2float_none(td4):
                                    bdfx='增持'
                                else:
                                    bdfx='减持'
                                    bdsl = -bdsl
    
                                #下行是为了修改600568数据的一条错误
                                if bdrq[:3]=='210':
                                    bdrq='201'+bdrq[3:]
                                    
                                data.append([my.lgpdm(gpdm),bdrq,bdr,bdsl,sygf,bdfx])
                        else:
                            #第一行
                            if i>0:
                                bdrq = td7
                                bdr = td1
                                bdsl = round(my.str2float_none(td3)*10000,0)
                                sygf = round(my.str2float_none(td4)*10000,0)
                                if my.str2float_none(td2)<my.str2float_none(td4):
                                    bdfx='增持'
                                else:
                                    bdfx='减持'
                                    bdsl = -bdsl
    
                                #下行是为了修改600568数据的一条错误
                                if bdrq[:3]=='210':
                                    bdrq='201'+bdrq[3:]
                                    
                                data.append([my.lgpdm(gpdm),bdrq,bdr,bdsl,sygf,bdfx])
    
                            td1 =cc[1]
                            td2 =cc[2]
                            td3 =cc[3]
                            td4 =cc[4]
                            td5 =cc[5]
                            td6 =cc[6]
                            td7 =cc[7]
                        
                        i = i+1

    except : 
        print("股东持股变动,出错退出")
    
    return data

    
'''
CREATE TABLE [XSJJ](
  [GPDM] TEXT NOT NULL, 
  [JJRQ] TEXT NOT NULL, 
  [JJSL] REAL NOT NULL, 
  [QLTGB] REAL, 
  [QLTBL] REAL, 
  [HLTGB] REAL, 
  [HLTBL] REAL, 
  [QZD] REAL, 
  [HZD] REAL, 
  [WLT] REAL);

CREATE UNIQUE INDEX [GPDM_JJRQ_XSJJ]
ON [XSJJ](
  [GPDM], 
  [JJRQ]);

        
CREATE TABLE [G_GGGDCGBD](
  [GPDM] TEXT NOT NULL, 
  [BDRQ] TEXT NOT NULL, 
  [BDR] TEXT NOT NULL, 
  [BDSL] REAL NOT NULL, 
  [SYGF] REAL NOT NULL, 
  [BDFX] TEXT NOT NULL);

CREATE UNIQUE INDEX [GPDM_BDRQ_SYGF_BDR_GGGDCGBD]
ON [G_GGGDCGBD](
  [GPDM], 
  [BDRQ],
  [SYGF], 
  [BDR]);

'''    

###############################################################################
#高管股东增减持统计
###############################################################################
def gggdzjctj():
    td=datetime.datetime.now()

    m0=td.strftime("%Y-%m-%d")
    m1=(td+datetime.timedelta(-30*6)).strftime("%Y-%m-%d")

    dbfn=my.getdrive()+'\\hyb\\STOCKDATA.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    sql='''select distinct gpdm from g_gggdcgbd 
               where bdrq>="%s" and bdrq<"%s" 
            ;'''  % (m1,m0)
    curs.execute(sql)        
    data = curs.fetchall()
    dbcn.close()
    
    cols=['gpdm']
    df = pd.DataFrame(data,columns=cols)
    df = df.set_index('gpdm',drop=False)

    #股票总数，当前股票序号，开始时间
    global stk_total,stk_n,start_time
    
    print('开始计算……')
    stk_total=len(df)
    
    #################################################################
    #增减持净额
    #################################################################

    start_time = time.time()
    stk_n=1
    
    
    zjchz=df['gpdm'].map(gggdzjc_sum)

    zjcdf=zjchz.to_frame()

    #高管股东增减持金额汇总，最近六个月、三个月、二个月、一个月，2周，1周，昨日
    zjcdf['m9zjc']=[e[0] for e in zjcdf['gpdm']]
    zjcdf['m6zjc']=[e[1] for e in zjcdf['gpdm']]
    zjcdf['m3zjc']=[e[2] for e in zjcdf['gpdm']]
    zjcdf['m2zjc']=[e[3] for e in zjcdf['gpdm']]
    zjcdf['m1zjc']=[e[4] for e in zjcdf['gpdm']]
    zjcdf['w2zjc']=[e[5] for e in zjcdf['gpdm']]
    zjcdf['w1zjc']=[e[6] for e in zjcdf['gpdm']]
    zjcdf['zrzjc']=[e[7] for e in zjcdf['gpdm']]

    zjcdf=zjcdf.drop(columns=['gpdm'])
    zjcdf=zjcdf.round(4)

    #转换NaN
    zjcdf=zjcdf.replace(0,np.nan) 

    csvfn = os.path.join(samepath(),'zjcdf.csv')

    zjcdf.to_csv(csvfn)

    #################################################################
    #增持
    #################################################################
    
#    #股票总数，当前股票序号，开始时间
#    global stk_total,stk_n,start_time
#    
#    print('开始计算……')
#    stk_total=len(df)
    
    start_time = time.time()
    stk_n=1
    
    zjchz=df['gpdm'].map(gggdzc_sum)

    zjcdf=zjchz.to_frame()

    #高管股东增持金额汇总，最近六个月、三个月、二个月、一个月，2周，1周，昨日
    zjcdf['m9zc']=[e[0] for e in zjcdf['gpdm']]
    zjcdf['m6zc']=[e[1] for e in zjcdf['gpdm']]
    zjcdf['m3zc']=[e[2] for e in zjcdf['gpdm']]
    zjcdf['m2zc']=[e[3] for e in zjcdf['gpdm']]
    zjcdf['m1zc']=[e[4] for e in zjcdf['gpdm']]
    zjcdf['w2zc']=[e[5] for e in zjcdf['gpdm']]
    zjcdf['w1zc']=[e[6] for e in zjcdf['gpdm']]
    zjcdf['zrzc']=[e[7] for e in zjcdf['gpdm']]

    zjcdf=zjcdf.drop(columns=['gpdm'])
    zjcdf=zjcdf.round(4)

    #转换NaN
    zjcdf=zjcdf.replace(0,np.nan) 

    csvfn = os.path.join(samepath(),'zcdf.csv')

    zjcdf.to_csv(csvfn)

    return        
    
###############################################################################
#高管股东增持统计汇总
###############################################################################
def gggdzc_sum(gpdm):
    
    global stk_total,stk_n,start_time    

#    print(gpdm)
    td=datetime.datetime.now()

    m0=td.strftime("%Y-%m-%d")
    m9=(td+datetime.timedelta(-30*9)).strftime("%Y-%m-%d")
    m6=(td+datetime.timedelta(-30*6)).strftime("%Y-%m-%d")
    m3=(td+datetime.timedelta(-30*3)).strftime("%Y-%m-%d")
    m2=(td+datetime.timedelta(-30*2)).strftime("%Y-%m-%d")
    m1=(td+datetime.timedelta(-30*1)).strftime("%Y-%m-%d")
    w2=(td+datetime.timedelta(-14)).strftime("%Y-%m-%d")
    w1=(td+datetime.timedelta(-7)).strftime("%Y-%m-%d")
    zr=lastopenday()
    zr=zr[:4]+'-'+zr[4:6]+'-'+zr[6:]
    
    dbfn=my.getdrive()+'\\hyb\\STOCKDATA.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    sql='''select bdrq,bdsl,bdfx from g_gggdcgbd 
               where gpdm="%s" and bdsl>0 and bdrq>="%s" and bdrq<"%s" 
            ;'''  % (my.lgpdm(gpdm),m9,m0)
    curs.execute(sql)        
    data = curs.fetchall()
    cols=['date','bdsl','bdfx']
    df = pd.DataFrame(data,columns=cols)
    df=df.set_index('date')    
    
    gjdf=get_fqgj(gpdm,'close')
    df=df.join(gjdf)    
    #计算变动金额，单位“亿元”
    df=df.assign(bdje=df['bdsl']*df['close']/100000000)
    m9sum=df.loc[m9:m0,'bdje'].sum()
    m6sum=df.loc[m6:m0,'bdje'].sum()
    m3sum=df.loc[m3:m0,'bdje'].sum()
    m2sum=df.loc[m2:m0,'bdje'].sum()
    m1sum=df.loc[m1:m0,'bdje'].sum()
    w2sum=df.loc[w2:m0,'bdje'].sum()
    w1sum=df.loc[w1:m0,'bdje'].sum()
    zrsum=df.loc[zr:m0,'bdje'].sum()

    
    #n为显示进度次数
    n=10
    m=int((stk_total+n-1)/n)
    
    if stk_n%m == 0 or stk_n>=stk_total:
        t1=time.time() - start_time
        p=t1/stk_n
        t1=t1/60
        t2=(stk_total-stk_n)*p/60
                
        print('正在进行增持统计，已完成%5.2f％用时%d分钟，估计还需要%d分钟' % ((stk_n/stk_total*100),t1,t2))

    stk_n += 1

    return [m9sum,m6sum,m3sum,m2sum,m1sum,w2sum,w1sum,zrsum]

###############################################################################
#高管股东增减持统计汇总
###############################################################################
def gggdzjc_sum(gpdm):
    
    global stk_total,stk_n,start_time    

#    print(gpdm)
    td=datetime.datetime.now()

    m0=td.strftime("%Y-%m-%d")
    m9=(td+datetime.timedelta(-30*9)).strftime("%Y-%m-%d")
    m6=(td+datetime.timedelta(-30*6)).strftime("%Y-%m-%d")
    m3=(td+datetime.timedelta(-30*3)).strftime("%Y-%m-%d")
    m2=(td+datetime.timedelta(-30*2)).strftime("%Y-%m-%d")
    m1=(td+datetime.timedelta(-30*1)).strftime("%Y-%m-%d")
    w2=(td+datetime.timedelta(-14)).strftime("%Y-%m-%d")
    w1=(td+datetime.timedelta(-7)).strftime("%Y-%m-%d")
    zr=lastopenday()
    zr=zr[:4]+'-'+zr[4:6]+'-'+zr[6:]
    
    dbfn=my.getdrive()+'\\hyb\\STOCKDATA.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    sql='''select bdrq,bdsl,bdfx from g_gggdcgbd 
               where gpdm="%s" and bdrq>="%s" and bdrq<"%s" 
            ;'''  % (my.lgpdm(gpdm),m9,m0)
    curs.execute(sql)        
    data = curs.fetchall()
    cols=['date','bdsl','bdfx']
    df = pd.DataFrame(data,columns=cols)
    df=df.set_index('date')    
    
    gjdf=get_fqgj(gpdm,'close')
    df=df.join(gjdf)    
    #计算变动金额，单位“亿元”
    df=df.assign(bdje=df['bdsl']*df['close']/100000000)
    m9sum=df.loc[m9:m0,'bdje'].sum()
    m6sum=df.loc[m6:m0,'bdje'].sum()
    m3sum=df.loc[m3:m0,'bdje'].sum()
    m2sum=df.loc[m2:m0,'bdje'].sum()
    m1sum=df.loc[m1:m0,'bdje'].sum()
    w2sum=df.loc[w2:m0,'bdje'].sum()
    w1sum=df.loc[w1:m0,'bdje'].sum()
    zrsum=df.loc[zr:m0,'bdje'].sum()

    
    #n为显示进度次数
    n=10
    m=int((stk_total+n-1)/n)
    
    if stk_n%m == 0 or stk_n>=stk_total:
        t1=time.time() - start_time
        p=t1/stk_n
        t1=t1/60
        t2=(stk_total-stk_n)*p/60
                
        print('正在进行增、减持统计，已完成%5.2f％用时%d分钟，估计还需要%d分钟' % ((stk_n/stk_total*100),t1,t2))

    stk_n += 1

    return [m9sum,m6sum,m3sum,m2sum,m1sum,w2sum,w1sum,zrsum]

###############################################################################
#前复权复权，复权因子是从tushare下载的csv文件
###############################################################################
def get_fqgj(gpdm,cols=None):

    data=tdxday2pd(gpdm)
    
    #未上市新股
    if isinstance(data,list):
        return []
    
    cols1=['date','adj_close']
    
    if len(gpdm)==6 and (gpdm[0] =='6' or gpdm[:2]=='00' or gpdm[:3] in ('002','300')):    
        #计算前复权因子
        adjf=factor_adj(gpdm)
        
        #前复权收盘价
        data=data.join(adjf)
        #补缺，可以解决tushare还没有更新，而当天收盘结束后缺失复权因子导致无法进行后续涨幅计算
        data['adj_factor'].fillna(method='ffill',inplace=True)
        data=data.eval('adj_close = adj_close * adj_factor')
    
    if cols!=None:    
        if isinstance(cols,list) or isinstance(cols,tuple):
            for col in cols:
                if col in data.columns and col not in cols1:
                    cols1.append(col)
        elif isinstance(cols,str):
            if cols in data.columns and cols not in cols1:
                cols1.append(cols)
            
    fqgj=data[cols1]
        
    '''
    pandas 将“字符类型的日期列”转化成“时间戳索引（DatetimeIndex）”
    https://www.jianshu.com/p/4ece5843d383
    '''
    fqgj=fqgj.set_index('date')    

    fqgj.index = pd.DatetimeIndex(fqgj.index)
    
    return fqgj.round(3)


###############################################################################
#复权因子是从tushare下载的csv文件
###############################################################################
def factor_adj(gpdm):
    df = pd.read_csv('http://file.tushare.org/tsdata/f/factor/%s.csv' % gpdm)
    df = df.set_index('datetime')
    df.index.name = 'rq'
    df=df.sort_index(ascending = False)
    a = df.iloc[0][0]
    df=df/a

    return df

###############################################################################
#前复权复权，复权因子是从tushare下载的csv文件
###############################################################################
def get_fqgj1(gpdm,cols=None):

    data=tdxday2pd(gpdm)
    
    #未上市新股
    if isinstance(data,list):
        return []
    
    cols1=['date','adj_close']
    
    if len(gpdm)==6 and (gpdm[0] =='6' or gpdm[:2]=='00' or gpdm[:3] in ('002','300')):    
        #计算前复权因子
        adjf=factor_adj(gpdm)
        
        #前复权收盘价
        data=data.join(adjf)
        data=data.eval('adj_close = adj_close * adj_factor')
    
    if cols!=None:    
        if isinstance(cols,list) or isinstance(cols,tuple):
            for col in cols:
                if col in data.columns and col not in cols1:
                    cols1.append(col)
        elif isinstance(cols,str):
            if cols in data.columns and cols not in cols1:
                cols1.append(cols)
            
    fqgj=data[cols1]
        
    '''
    pandas 将“字符类型的日期列”转化成“时间戳索引（DatetimeIndex）”
    https://www.jianshu.com/p/4ece5843d383
    '''
    fqgj=fqgj.set_index('date')    

    fqgj.index = pd.DatetimeIndex(fqgj.index)
    
    return fqgj.round(3)

###############################################################################
#将通达信.day读入pands。
#gpdm形如：600027，sh600027,600027.sh
#对于6位数字的代码赚表示A股代码
#对于sh000001上证指数、sz399300沪深300指数则直接        
###############################################################################
def tdxday2pd(gpdm,start=None,end=None):
    if len(gpdm)==8 and gpdm[:2] in ('sh','sz'):
        sc=gpdm[:2]
        gpdm=gpdm[2:]
        dayfn =my.getdrive()+'\\tdx\\'+sc+'lday\\'+sc+gpdm+'.day'
    else:
        gpdm=my.sgpdm(gpdm)
        sc = 'sh' if gpdm[0]=='6' else 'sz'
        dayfn =my.getdrive()+'\\tdx\\'+sc+'lday\\'+sc+gpdm+'.day'

    if os.path.exists(dayfn) :
        return day2pd(dayfn,start,end)
    else :
        return []

###############################################################################
#将通达信.day读入pands
###############################################################################
def day2pd(dayfn,start=None,end=None):
    
    if end == None:
        end=datetime.datetime.now().strftime('%Y%m%d')
    if start == None:
        start='20080101'

    columns = ['rq','date','open', 'high', 'low','close','amout','volume','rate','pre_close','adj_rate','adj_close']

    with open(dayfn,"rb") as f:
        data = f.read()
        f.close()
    days = int(len(data)/32)
    records = []
    qsp = 0
    for i in range(days):
        dat = data[i*32:(i+1)*32]
        rq,kp,zg,zd,sp,cje,cjl,tmp = struct.unpack("iiiiifii", dat)
        if rq==0 or rq<int(start):
            continue
#        print(days,i,rq)
        rq1 = str2datetime(str(rq))
        rq2 = rq1.strftime("%Y-%m-%d")
        kp = kp/100.00
        zg = zg/100.00
        zd = zd/100.00
        sp = sp/100.00
        cje = cje/100000000.00     #亿元
        cjl = cjl/10000.00         #万股
        zf = sp/qsp-1 if (i>0 and qsp>0) else 0.0
        records.append([rq1,rq2,kp,zg,zd,sp,cje,cjl,zf,qsp,zf,sp])
        qsp = sp

    df = pd.DataFrame(records,columns=columns)
    df = df.set_index('rq')
    start = str2datetime(start)
    end = str2datetime(end)

    if start == None or end==None :
        return df
    else :
        return df[start:end]

##########################################################################
#将字符串转换为时间戳，不成功返回None
##########################################################################
def str2datetime(s):
    if s is None:
        return None
    if ('-' in s) or ('/' in s):
        if '-' in s:
            dt=s.split('-')
        if '/' in s:
            dt=s.split('/')        
        try:
            dt = datetime.datetime(int(dt[0]),int(dt[1]),int(dt[2]))
        except :
            dt = None

    if len(s)==8:
        try:
            dt = datetime.datetime(int(s[:4]),int(s[4:6]),int(s[6:8]))
        except :
            dt = None

    return dt


##########################################################################
#读取高管股东增减持信息
##########################################################################
def read_gggdzjc():
    
    global start_time,lastnum

    gpdmb=my.get_gpdm()
    #上市日期、行业、总股本、每股净资产
    gpssrq=my.get_stock_basics()
    
    gpdmb=gpdmb.join(gpssrq)
    
    #去掉还未上市的
#    gpdmb=gpdmb.loc[(~(gpdmb['ssrq'].isna()))]
    gpdmb=gpdmb.loc[(~(gpdmb['ssrq'].isna()) & (gpdmb['ssrq']<'20190101'))]
    
#    sys.exit()
    
    
    dbfn=my.getdrive()+'\\hyb\\STOCKDATA.db'
    dbcn = sqlite3.connect(dbfn)
    
    j=lastnum
    for i in range(j,len(gpdmb)):
        gpdm=gpdmb.index[i]
        gpmc = gpdmb.iloc[i]['gpmc']
        print("共有%d只股票，正在处理第%d只：%s%s，请等待…………" % (len(gpdmb),i,gpdm,gpmc)) 

        #保存当前进度
        config['lastnum'] = i
        config.write()

        data = get_gdcgbd1(gpdm)
        
        if len(data)>0 :
            dbcn.executemany('''INSERT OR REPLACE INTO G_GGGDCGBD (GPDM,BDRQ,BDR,BDSL,SYGF,BDFX)
            VALUES (?,?,?,?,?,?)''', data)

        data = get_ggcgbd1(gpdm)
        
        if len(data)>0 :
            dbcn.executemany('''INSERT OR REPLACE INTO G_GGGDCGBD (GPDM,BDRQ,BDR,BDSL,SYGF,BDFX)
            VALUES (?,?,?,?,?,?)''', data)

        if ((i+1) % 10 ==0) or i>=len(gpdmb)-1 :
            now_time = time.time()
            t1 = now_time - start_time
            #每只股票秒数            
            p = t1/(i-j+1)
            #估计剩余时间
            t1 = t1/60
            t2 = (len(gpdmb) - i)*p/60
            now = datetime.datetime.now().strftime('%H:%M:%S')

            print('------已用时%d分钟，估计还需要%d分钟(%s)' % (t1,t2,now))

            dbcn.commit()

        if i>=len(gpdmb)-1 :
            #正常结束，将进度值重置为0
            config['lastnum'] = 0    
            config.write()
        
    dbcn.close()

    return

###############################################################################
#获取最新交易日，如果当天是交易日，在18:00后用当天，如果当天不是交易日
###############################################################################
def lastopenday():

    df = pd.read_csv(r'd:\selestock\calAll.csv', dtype={'calendarDate':'object'})

    days=df.loc[df['isOpen']==1,'calendarDate']
    days=[e.replace('-','') for e in days]
    days.sort()
    now = datetime.datetime.now()
    td = now.strftime("%Y%m%d") #今天
    i=0
    while True :
        if td<=days[i]:
            break
        else:
            i += 1
    #不是交易日或者是交易日但在16点以前，则取前一个交易日
    if td!=days[i] or now.strftime("%H")<'16':
        i = i-1

    return days[i]    

if __name__ == "__main__":  
#    print(samepath())
#    sys.exit()
#def temp():
    
    config = my.iniconfig()
    lastnum = my.readkey(config,'lastnum',0)
    
#    print(lastnum)
#    sys.exit()
    
    if isinstance(lastnum,str):
        lastnum=int(lastnum)


    #https://blog.csdn.net/wangchuner/article/details/83061947
    #【Python爬虫错误】ConnectionResetError: [WinError 10054] 远程主机强迫关闭了一个现有的连接
    socket.setdefaulttimeout(20)  # 设置socket层的超时时间为20秒

    global start_time
    start_time = time.time()
        
    now1 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now1)

#    fireFoxOptions = webdriver.FirefoxOptions()
#    fireFoxOptions.headless=True
#    browser = webdriver.Firefox(options=fireFoxOptions)

    move = dict.fromkeys((ord(c) for c in u"\xa0\t"))

    read_gggdzjc()

#    browser.quit()    

    #高管股东增减持统计
    gggdzjctj()
    
    now2 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now1)
    print('结束运行时间：%s' % now2)

'''
python使用pyquery库总结 
https://blog.csdn.net/baidu_21833433/article/details/70313839

python socket.error: [Errno 10054] 远程主机强迫关闭了一个现有的连接。问题解决方案
https://blog.csdn.net/weixin_42350212/article/details/80570358


'''
