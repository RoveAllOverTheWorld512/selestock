# -*- coding: utf-8 -*-
"""
Created on Tue Aug 28 10:23:19 2018

@author: lenovo
"""

import os
import sys
import datetime
import re
import pandas as pd
import struct

###############################################################################
#长股票代码
###############################################################################
def lgpdm(dm):
    dm=re.findall('(\d{6})',dm)
    
    if len(dm)==0 :
        return None

    dm=dm[0] 

    return dm+('.SH' if dm[0]=='6' else '.SZ')

###############################################################################
#中股票代码
###############################################################################
def mgpdm(dm):
    dm=re.findall('(\d{6})',dm)
    
    if len(dm)==0 :
        return None
    dm=dm[0]
    return ('SH' if dm[0]=='6' else 'SZ')+dm

###############################################################################
#短股票代码
###############################################################################
def sgpdm(dm):
    dm=re.findall('(\d{6})',dm)
    
    if len(dm)==0 :
        return None

    return dm[0]

##########################################################################
#获取运行程序所在驱动器
##########################################################################
def getdrive():
    if sys.argv[0]=='' :
        return os.path.splitdrive(os.getcwd())[0]
    else:
        return os.path.splitdrive(sys.argv[0])[0]


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
        dayfn =getdrive()+'\\tdx\\'+sc+'lday\\'+sc+gpdm+'.day'
    else:
        gpdm=sgpdm(gpdm)
        sc = 'sh' if gpdm[0]=='6' else 'sz'
        dayfn =getdrive()+'\\tdx\\'+sc+'lday\\'+sc+gpdm+'.day'

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

