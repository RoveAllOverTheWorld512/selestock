# -*- coding: utf-8 -*-
"""
Created on Sun Dec  9 09:22:54 2018

@author: lenovo

板块涨幅统计

"""

import os
import sys
import re
import datetime
from configobj import ConfigObj
import sqlite3
import numpy as np
import pandas as pd
import xlwings as xw
import struct
import winreg
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pyquery import PyQuery as pq
import time
import tushare as ts
from urllib import request
import zipfile
from bs4 import BeautifulSoup as bs
import dateutil.parser
import xlrd
import subprocess


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
#获取本机通达信安装目录，生成自定义板块保存目录
########################################################################
def gettdxdir():

    try :
        key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE,r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\华西证券华彩人生")
        value, type = winreg.QueryValueEx(key, "InstallLocation")
    except :
        print("本机未安装【华西证券华彩人生】软件系统。")
        sys.exit()
    return value

########################################################################
#获取本机通达信安装目录，生成自定义板块保存目录
########################################################################
def gettdxblkdir():
    try :
        key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE,r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\华西证券华彩人生")
        value, type = winreg.QueryValueEx(key, "InstallLocation")
        return value + '\\T0002\\blocknew'
    except :
        print("本机未安装【华西证券华彩人生】软件系统。")
        sys.exit()

###############################################################################
#从通达信系统读取股票代码表
###############################################################################
def get_gpdm():
    datacode = []
    for sc in ('h','z'):
        fn = gettdxdir()+'\\T0002\\hq_cache\\s'+sc+'m.tnf'
        f = open(fn,'rb')
        f.seek(50)
        ss = f.read(314)
        while len(ss)>0:
            gpdm=ss[0:6].decode('GBK')
            gpmc=ss[23:31].strip(b'\x00').decode('GBK').replace(' ','').replace('*','')
            gppy=ss[285:291].strip(b'\x00').decode('GBK')
            #剔除非A股代码
            if (sc=="h" and gpdm[0]=='6') :
                gpdm=gpdm+'.SH'
                datacode.append([gpdm,gpmc,gppy])
            if (sc=='z' and (gpdm[0:2]=='00' or gpdm[0:2]=='30')) :
                gpdm=gpdm+'.SZ'
                datacode.append([gpdm,gpmc,gppy])
            ss = f.read(314)
        f.close()
    gpdmb=pd.DataFrame(datacode,columns=['gpdm','gpmc','gppy'])
    gpdmb['dm']=gpdmb['gpdm'].map(lambda x:x[:6])
    gpdmb=gpdmb.set_index('gpdm',drop=False)
    return gpdmb

########################################################################
#获取本机通达信安装目录，生成自定义板块保存目录
########################################################################
def gettdxblk(lb):

    try :
        key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE,r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\华西证券华彩人生")
        value, type = winreg.QueryValueEx(key, "InstallLocation")
    except :
        print("本机未安装【华西证券华彩人生】软件系统。")
        sys.exit()

    blkfn = value + '\\T0002\\hq_cache\\block_'+lb+'.dat'
    blk = {}
    with open(blkfn,'rb') as f :
        blknum, = struct.unpack('384xH', f.read(386))
        for i in range(blknum) :
            stk = []
            blkname = f.read(9).strip(b'\x00').decode('GBK')
            stnum, = struct.unpack('H2x', f.read(4))
            for j in range(stnum) :
                stkid = f.read(7).strip(b'\x00').decode('GBK')
                stk.append(stkid)
            blk[blkname] = [blkname,stnum,stk]

            f.read((400-stnum)*7)
            
        f.close()


    return blk

#############################################################################
#股票列表,通达信板块文件调用时wjtype="tdxbk"
#############################################################################
def zxglist(zxgfn,wjtype=""):
    zxglst = []
    p = "(\d{6})"
    if wjtype == "tdxblk" :
        p ="\d(\d{6})"
    if os.path.exists(zxgfn) :
        #用二进制方式打开再转成字符串，可以避免直接打开转换出错
        with open(zxgfn,'rb') as dtf:
            zxg = dtf.read()
            if zxg[:3] == b'\xef\xbb\xbf' :
                zxg = zxg.decode('UTF8','ignore')   #UTF-8
            elif zxg[:2] == b'\xfe\xff' :
                zxg = zxg.decode('UTF-16','ignore')  #Unicode big endian
            elif zxg[:2] == b'\xff\xfe' :
                zxg = zxg.decode('UTF-16','ignore')  #Unicode
            else :
                zxg = zxg.decode('GBK','ignore')      #ansi编码
        zxglst =re.findall(p,zxg)
    else:
        print("文件%s不存在！" % zxgfn)
    if len(zxglst)==0:
        print("股票列表为空,请检查%s文件。" % zxgfn)

    zxg = list(set(zxglst))
    zxg.sort(key=zxglst.index)

    return zxg

#############################################################################
#通达信自选股A股列表，去掉了指数代码
#############################################################################    
def zxglst(zxgfile=None):

    if zxgfile==None:
        zxgfile="zxg.blk"
    else:
        if '.blk' not in zxgfile:
            zxgfile=zxgfile+'.blk'
            
    tdxblkdir = gettdxblkdir()
    zxgfile = os.path.join(tdxblkdir,zxgfile)
    if not os.path.exists(zxgfile):
        print("板块不存在，请检查！")
        return pd.DataFrame()
    
    zxg = zxglist(zxgfile,"tdxblk")
    
    gpdmb=get_gpdm()
    
    #去掉指数代码只保留A股代码
    zxglb=gpdmb.loc[gpdmb['dm'].isin(zxg),:]
    #增加一列
    #http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.assign.html
    zxglb=zxglb.assign(no=zxglb['dm'].map(lambda x:zxg.index(x)+1))

    zxglb=zxglb.set_index('no') 
    zxglb=zxglb.sort_index()       
    return zxglb


##########################################################################
#获取运行程序所在驱动器
##########################################################################
def getdrive():
    if sys.argv[0]=='' :
        return os.path.splitdrive(os.getcwd())[0]
    else:
        return os.path.splitdrive(sys.argv[0])[0]



#############################################################################
#读取中证行业分类   
#############################################################################
def get_zzhybk():

    files = os.listdir(getdrive()+'\\syl')
    fs = [re.findall('csi(\d{8})\.xls',e) for e in files]
    jyrlist =[]
    for e in fs:
        if len(e)>0:
            file = getdrive()+'\\syl\\csi'+e[0]+'.xls'
            #剔除长度为0的文件
            if os.path.getsize(file)>0:        
                jyrlist.append(e[0])

    jyrlist=sorted(jyrlist,reverse=1)
    file = getdrive()+'\\syl\\csi'+jyrlist[0]+'.xls'
    wb = xlrd.open_workbook(file,encoding_override="cp1252")
    table = wb.sheet_by_name('个股数据')
    nrows = table.nrows #行数

    data = []
    for rownum in range(1,nrows):
        row = table.row_values(rownum)
        data.append([lgpdm(row[0]),row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9]])
        
    cols=['ts_code','gpmc','zz_hy1dm','zz_hy1mc','zz_hy2dm','zz_hy2mc','zz_hy3dm','zz_hy3mc','zz_hy4dm','zz_hy4mc']    
    df=pd.DataFrame(data,columns=cols)
    
    blks1={}
    blks2={}
    blks3={}
    blks4={}
    for index, row in df.iterrows():
        key1='%s%s' % (row[2],row[3])
        if key1 in blks1.keys():
            blks1[key1][2].append(row[0])
            blks1[key1][1]=len(blks1[key1][2])
        else:
            blks1[key1]=[key1,1,[row[0]]]
            
        key2='%s%s' % (row[4],row[5])
        if key2 in blks2.keys():
            blks2[key2][2].append(row[0])
            blks2[key2][1]=len(blks2[key2][2])
        else:
            blks2[key2]=[key2,1,[row[0]]]
    
        key3='%s%s' % (row[6],row[7])
        if key3 in blks3.keys():
            blks3[key3][2].append(row[0])
            blks3[key3][1]=len(blks3[key3][2])
        else:
            blks3[key3]=[key3,1,[row[0]]]

        key4='%s%s' % (row[8],row[9])
        if key4 in blks4.keys():
            blks4[key4][2].append(row[0])
            blks4[key4][1]=len(blks4[key4][2])
        else:
            blks4[key4]=[key4,1,[row[0]]]

    return [blks1,blks2,blks3,blks4]

#############################################################################
#读取证监会行业分类
#############################################################################
def get_zjhhybk():

    files = os.listdir(getdrive()+'\\pe')
    fs = [re.findall('(\d{8})\.xls',e) for e in files]
    jyrlist =[]
    for e in fs:
        if len(e)>0:
            file = getdrive()+'\\pe\\'+e[0]+'.xls'
            #剔除长度为0的文件
            if os.path.getsize(file)>0:        
                jyrlist.append(e[0])

    jyrlist=sorted(jyrlist,reverse=1)

    file = getdrive()+'\\pe\\'+jyrlist[0]+'.xls'
    wb = xlrd.open_workbook(file,encoding_override="cp1252")
    table = wb.sheet_by_name('个股数据')
    nrows = table.nrows #行数

    data = []
    for rownum in range(1,nrows):
        row = table.row_values(rownum)

        data.append([lgpdm(row[0]),row[1],row[2],row[3],row[4],row[5]])
        
    cols=['ts_code','gpmc','zjh_mldm','zjh_mlmc','zjh_dldm','zjh_dlmc']    
    df=pd.DataFrame(data,columns=cols)

    blks1={}
    blks2={}
    for index, row in df.iterrows():
        key1='%s%s' % (row[2],row[3])
        if key1 in blks1.keys():
            blks1[key1][2].append(row[0])
            blks1[key1][1]=len(blks1[key1][2])
        else:
            blks1[key1]=[key1,1,[row[0]]]
            
        key2='%s%s%s' % (row[2],row[4],row[5])
        if key2 in blks2.keys():
            blks2[key2][2].append(row[0])
            blks2[key2][1]=len(blks2[key2][2])
        else:
            blks2[key2]=[key2,1,[row[0]]]
    
    return [blks1,blks2]

########################################################################
# 根据通达信新行业或申万行业代码提取股票列表
# https://blog.csdn.net/liuyukuan/article/details/79483812
########################################################################
def tdxswhy():

    fn=gettdxdir()+'incon.dat'
    with open(fn,'rb') as dtf:
        zxg = dtf.read()
        if zxg[:3] == b'\xef\xbb\xbf' :
            zxg = zxg.decode('UTF8','ignore')   #UTF-8
        elif zxg[:2] == b'\xfe\xff' :
            zxg = zxg.decode('UTF-16','ignore')  #Unicode big endian
        elif zxg[:2] == b'\xff\xfe' :
            zxg = zxg.decode('UTF-16','ignore')  #Unicode
        else :
            zxg = zxg.decode('GBK','ignore')      #ansi编码
   
        dtf.close()
        
    p='#TDXNHY(.*?)######'         
    tdxhy=re.findall(p,zxg,re.DOTALL)
    
    tdxhy=tdxhy[0].replace('|','\t')

    p='(.+)\t(.+)\r\n'
    tdxhy=re.findall(p,tdxhy)

    cols=['tdx_hydm','tdx_hymc']
    tdxdf=pd.DataFrame(tdxhy,columns=cols)


    p='#SWHY(.*?)######'         
    swhy=re.findall(p,zxg,re.DOTALL)
    
    swhy=swhy[0].replace('|','\t')

    p='(.+)\t(.+)\r\n'
    swhy=re.findall(p,swhy)
    
    cols=['sw_hydm','sw_hymc']
    swdf=pd.DataFrame(swhy,columns=cols)
    
    p = '(\d{6})\t(.+)\t(.+)\t(.+)\r\n'
    zxgfn = gettdxdir()+r'T0002\hq_cache\tdxhy.cfg'
    with open(zxgfn,'rb') as dtf:
        zxg = dtf.read()
        if zxg[:3] == b'\xef\xbb\xbf' :
            zxg = zxg.decode('UTF8','ignore')   #UTF-8
        elif zxg[:2] == b'\xfe\xff' :
            zxg = zxg.decode('UTF-16','ignore')  #Unicode big endian
        elif zxg[:2] == b'\xff\xfe' :
            zxg = zxg.decode('UTF-16','ignore')  #Unicode
        else :
            zxg = zxg.decode('GBK','ignore')      #ansi编码
   
        dtf.close()

    zxg=zxg.replace('|','\t')
    zxglst =re.findall(p,zxg)


    dt = [[lgpdm(gpdm),tdxnhy,swhy] for gpdm,tdxnhy,swhy,wzhy in zxglst]
    cols=['ts_code','tdx_hydm','sw_hydm']
    df=pd.DataFrame(dt,columns=cols)

    df=pd.merge(df,tdxdf,on='tdx_hydm')
    df=pd.merge(df,swdf,on='sw_hydm')
    
    return df


##########################################################################
#股票列表
##########################################################################
def get_stklst():
    
#    mytoken='18fcea168f6c1f8621c13bef376e726cf5e31fde3f579db37929181b'
#    pro = ts.pro_api(token=mytoken)
    #df = pro.daily(trade_date='20181206')
    
    #data = pro.stock_basic(exchange='', list_status='D', fields='ts_code,symbol,name,area,industry,list_date')
    
    dt0 = pro.stock_basic(list_status='L',fields='ts_code,symbol,name,area,industry,list_date')
    dt0=dt0.set_index('ts_code',drop=False)
    
    dt1 = pro.stock_basic(list_status='P',fields='ts_code,symbol,name,area,industry,list_date')
    dt1=dt1.set_index('ts_code',drop=False)
    
    dt2 = pro.stock_basic(list_status='D',fields='ts_code,symbol,name,area,industry,list_date')
    dt2=dt2.set_index('ts_code',drop=False)
    
    dt=pd.concat([dt0,dt1,dt2])
        
    dt=dt[~dt.index.duplicated()]

    return dt[['ts_code','name']]

##########################################################################
#
##########################################################################
def get_stknm(gpdm):
        
#    mytoken='18fcea168f6c1f8621c13bef376e726cf5e31fde3f579db37929181b'
#    pro = ts.pro_api(token=mytoken)
    return pro.namechange(ts_code=gpdm, fields='ts_code,name,start_date,end_date,change_reason')


##########################################################################
#
##########################################################################
def get_fqgj(gpdm):
#    #获取指定
    gpdm='600198.SH'
    df = pro.daily(ts_code=gpdm,start_date='20010101',end_date='20171231')
    fqyz = pro.adj_factor(ts_code=gpdm)
    
    fqgj=pd.merge(df, fqyz, on='trade_date',suffixes=('_x', '_y'))
    fqgj=fqgj[['trade_date', 'close','adj_factor']]
    zxyz=fqgj.iloc[0].adj_factor
    fqgj=fqgj.assign(close_adj=fqgj['close']*(fqgj['adj_factor']/zxyz))

    return fqgj

##########################################################################
#计算前复权因子，rq1<rq2，即日期1在前，日期2在后
##########################################################################
def get_qfqyz(rq1,rq2):
    #前复权因子
    adj1 = pro.adj_factor(ts_code='', trade_date=rq1)
    adj2 = pro.adj_factor(ts_code='', trade_date=rq2)
    adj = pd.merge(adj2, adj1, on='ts_code',suffixes=('_x', '_y'))
    adj = adj.assign(qfq = adj['adj_factor_y'] / adj['adj_factor_x'])
    adj = adj[['ts_code','qfq']]
    return adj


##########################################################################
#计算涨幅,ds=0表示不含停牌数据，ds=1表示使用本地数据含停牌数据
##########################################################################
def get_zf(rq1,rq2,ds=0):
    
    if ds==0:
        gj1 = pro.daily(trade_date=rq1)
        gj2 = pro.daily(trade_date=rq2)
    else:
        csvfn1=getdrive()+'\\tdx\\day\\%s.csv' % rq1
        csvfn2=getdrive()+'\\tdx\\day\\%s.csv' % rq2
        gj1 = pd.read_csv(csvfn1)
        gj2 = pd.read_csv(csvfn2)
        
    
    gj = pd.merge(gj2, gj1, on='ts_code',suffixes=('_x', '_y'))
    
    gj = gj[['ts_code', 'trade_date_x', 'close_x','trade_date_y', 'close_y']]

    zf = gj.assign(zdf=gj['close_x']/gj['close_y'])
    
    #获取前复权因子
    qfq=get_qfqyz(rq1,rq2)
    
    qfqzf=pd.merge(zf,qfq,on='ts_code')
    qfqzf=qfqzf.assign(zf=qfqzf['zdf']/qfqzf['qfq']*100.00-100.00)
    qfqzf=qfqzf.sort_values(by='zf',ascending=False)

    jbxx = pro.stock_basic(fields='ts_code,name,industry,list_date')
    qfqzf=pd.merge(qfqzf,jbxx,on='ts_code')
    qfqzf=qfqzf.round(2)
    
    return qfqzf[['ts_code','name','list_date','industry','zf']]
    
##########################################################################
#
##########################################################################
def get_tradedate(rq):

    mytoken='18fcea168f6c1f8621c13bef376e726cf5e31fde3f579db37929181b'
    pro = ts.pro_api(token=mytoken)
    
    #cal.csv保存股市日历
    calfn=r'd:\selestock\cal.csv'

    if not os.path.exists(calfn):

        cal=pro.trade_cal(start_date='19901219',end_date='20180505',fields='cal_date,is_open,pretrade_date')
        cal=cal.append(pro.trade_cal(start_date='20180506',end_date='20191231',fields='cal_date,is_open,pretrade_date'))
    
        cal=cal.sort_values(by='cal_date',ascending=False)

        cal.to_csv(calfn,index=False)

    else:

        cal= pd.read_csv(calfn, dtype={'cal_date':'object','pretrade_date':'object'})
    
    #下午4点以前日期用前一天，    
    if datetime.datetime.now().hour<16:
        today=(datetime.datetime.now()-datetime.timedelta(1)).strftime("%Y%m%d")
    else:
        today=datetime.datetime.now().strftime("%Y%m%d")

    if rq>today:
        rq=today
        
    
    #查找日期对于的index
    i=cal[cal['cal_date']==rq].index[0]

    if cal['is_open'][i]==0 :
        rq=cal.iloc[i].pretrade_date

    return rq    
    
##########################################################################
#获取ST板块
##########################################################################
def get_stbk():
#    gpdmb=get_stklst()
#    dt=get_stknm(gpdmb.iloc[0].ts_code)
#    for i in range(1,len(gpdmb)):
#        gpdm=gpdmb.iloc[i].ts_code
#        
#        print(gpdm)
#        df=get_stknm(gpdm)
#        dt=dt.append(df)
#    
#    dt.to_csv(r'd:\selestock\gpgm.csv',encoding='GBK',index=False)
    
    dt = pd.read_csv(r'd:\selestock\gpgm.csv',encoding='GBK',dtype='object')
    
    dt1=dt[dt['start_date']<rq1]
    dt1=dt1.sort_values(by='start_date',ascending=False)

#    股票代码去重    
    dt2=dt1[~dt1['ts_code'].duplicated()]

    #提取股票名称包含ST
    dt3=dt2[(dt2['name'].str.contains('ST'))]
    stbk=[gpdm for gpdm in dt3['ts_code']]
    stbk=['ST板块',len(stbk),stbk]

    return stbk

##########################################################################
#获取次新股板块，把开始日期前一年内上市的定义为次新股
##########################################################################
def get_cxgbk():

    #把开始日期前一年内上市的定义为次新股
    rq0=(dateutil.parser.parse(rq1)-datetime.timedelta(365)).strftime("%Y%m%d")
    jbxx = pro.stock_basic(fields='ts_code,name,industry,list_date')
    cxgbk=jbxx[jbxx['list_date']>rq0]
    cxgbk=[gpdm for gpdm in cxgbk['ts_code']]
    cxgbk=['次新股',len(cxgbk),cxgbk]

    return cxgbk

##########################################################################
#涨幅统计
#qfqzf    
##########################################################################
def get_zftj(blks):
    
    blkzf=[]
    
    for blk in blks.values():
        blkstk=[lgpdm(gpdm) for gpdm in blk[2]]

        if '次新股' in blks.keys():
            if blk[0]=='次新股':
                blkggzf=qfqzf[qfqzf['ts_code'].isin(blkstk)]
            else:
                blkggzf=qfqzf[(qfqzf['ts_code'].isin(blkstk) & ~qfqzf['ts_code'].isin(blks['次新股'][2]))]
        else:
            blkggzf=qfqzf[qfqzf['ts_code'].isin(blkstk)]

        blkggzf=blkggzf.sort_values(by='zf',ascending=False)

        n=len(blkggzf)
        #次新股板块与上市日期有关        
        if n>0 :
            gpdms=''
            for gpdm in blkggzf['ts_code']:
                gpdms=gpdms+'|'+gpdm[:6]
            h=blkggzf['zf'].max()
            i=blkggzf[(blkggzf['zf']==h)].index[0]
            
            hdm=blkggzf['ts_code'][i]
            hmc=blkggzf['name'][i]
            
            l=blkggzf['zf'].min()
            i=blkggzf[(blkggzf['zf']==l)].index[0]
            ldm=blkggzf['ts_code'][i]
            lmc=blkggzf['name'][i]
    
            blkpjzf=blkggzf['zf'].mean()
            blkzwzf=blkggzf['zf'].median()
            
            blkzf.append([blk[0],n,blkpjzf,blkzwzf,hdm,hmc,h,ldm,lmc,l,gpdms])
    
    cols=['blkname','num','pjzf','zwzf','max_dm','max_mc','max_zf','min_dm','min_mc','min_zf','gpdms']
    df=pd.DataFrame(blkzf,columns=cols)

    df=df.sort_values(by='zwzf',ascending=False) 

    h=qfqzf['zf'].max()
    i=qfqzf[(qfqzf['zf']==h)].index[0]
    
    hdm=qfqzf['ts_code'][i]
    hmc=qfqzf['name'][i]
    
    l=qfqzf['zf'].min()
    i=qfqzf[(qfqzf['zf']==l)].index[0]
    ldm=qfqzf['ts_code'][i]
    lmc=qfqzf['name'][i]

    pjzf=qfqzf['zf'].mean()
    zwzf=qfqzf['zf'].median()

    allzf=[['全部股票',len(qfqzf),pjzf,zwzf,hdm,hmc,h,ldm,lmc,l,'']]
    cols=['blkname','num','pjzf','zwzf','max_dm','max_mc','max_zf','min_dm','min_mc','min_zf','gpdms']
    df1=pd.DataFrame(allzf,columns=cols)

    df=df.append(df1)
    
    df=df.round(2)

    return df
    
##########################################################################
#通达信、申万行业板块
##########################################################################
def get_tdxswhybk():
    blks1={}
    blks2={}
    df=tdxswhy()
    for index, row in df.iterrows():
        tdxkey='%s%s' % (row[1],row[3])
        swkey='%s%s' % (row[2],row[4])
        if tdxkey in blks1.keys():
            blks1[tdxkey][2].append(row[0])
            blks1[tdxkey][1]=len(blks1[tdxkey][2])
        else:
            blks1[tdxkey]=[tdxkey,1,[row[0]]]
            
        if swkey in blks2.keys():
            blks2[swkey][2].append(row[0])
            blks2[swkey][1]=len(blks2[swkey][2])
        else:
            blks2[swkey]=[swkey,1,[row[0]]]
            
    return [blks1,blks2]
    
##########################################################################
#股票所属概念
##########################################################################
def get_gpgn():
    blks=gettdxblk('gn')
    gpgn={}
    for i in blks:
        for k in blks[i][2]:
            k=lgpdm(k)
            if k in gpgn.keys():
                gpgn[k][1]= gpgn[k][1]+1
                gpgn[k][2]='%s,%s' % (gpgn[k][2],i)
            else:
                gpgn[k]=[k,1,i]
    
    df=pd.DataFrame.from_dict(gpgn,orient='index',columns=['ts_code','gngs','gnmc'])        
    return df

if __name__ == '__main__':
    
#    sys.exit()
    
    #tushare 通过Python SDK 调取数据
    #https://tushare.pro/document/1?doc_id=131
    
    mytoken='18fcea168f6c1f8621c13bef376e726cf5e31fde3f579db37929181b'
    pro = ts.pro_api(token=mytoken)

    rq1 = get_tradedate('20190308')
    rq2 = get_tradedate('20190331')
    if rq1>=rq2 :
        print('起始日期必须小于截止日期')
        sys.exit()
        

    qfqzf=get_zf(rq1,rq2,1)

    
    blks=gettdxblk('gn')
    blks['ST板块']=get_stbk()
    blks['次新股']=get_cxgbk()
    
    gndf=get_zftj(blks)
    
    tdxswhybk=get_tdxswhybk()
    tdxdf=get_zftj(tdxswhybk[0])
    swdf=get_zftj(tdxswhybk[1])
    
    zzbk=get_zzhybk()
    zz1df=get_zftj(zzbk[0])
    zz2df=get_zftj(zzbk[1])
    zz3df=get_zftj(zzbk[2])
    zz4df=get_zftj(zzbk[3])
    
    zjhbk=get_zjhhybk()
    zjh1df=get_zftj(zjhbk[0])
    zjh2df=get_zftj(zjhbk[1])
    
    headdf=qfqzf.head(200).copy()
    headdf=headdf[['ts_code','name','zf','list_date']]
    
    taildf=qfqzf.tail(200).copy()
    taildf=taildf[['ts_code','name','zf','list_date']]
    taildf=taildf.sort_values(by='zf',ascending=True)
    
    gpgndf=get_gpgn()
    qfqzf=pd.merge(qfqzf,gpgndf,how='left',on='ts_code')
    
    fn=r'd:\selestock\板块涨幅_%s_%s.xlsx' % (rq1,rq2)

    writer=pd.ExcelWriter(fn,engine='xlsxwriter')

    gndf.to_excel(writer, sheet_name='通达信概念板块',index=False)   
    tdxdf.to_excel(writer, sheet_name='通达信行业板块',index=False)   

    swdf.to_excel(writer, sheet_name='申万行业板块',index=False)   

    zjh1df.to_excel(writer, sheet_name='证监会行业门类板块',index=False)   
    zjh2df.to_excel(writer, sheet_name='证监会行业大类板块',index=False)   

    zz1df.to_excel(writer, sheet_name='中证一级行业板块',index=False)   
    zz2df.to_excel(writer, sheet_name='中证二级行业板块',index=False)   
    zz3df.to_excel(writer, sheet_name='中证三级行业板块',index=False)   
    zz4df.to_excel(writer, sheet_name='中证四级行业板块',index=False)   

    headdf.to_excel(writer, sheet_name='涨幅最大个股',index=False)   
    taildf.to_excel(writer, sheet_name='跌幅最大个股',index=False) 

    qfqzf.to_excel(writer, sheet_name='全部个股',index=False) 
    

    writer.save()

