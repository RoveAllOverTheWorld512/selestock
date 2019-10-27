# -*- coding: utf-8 -*-
"""
Created on Mon Jul  9 17:02:48 2018

@author: lenovo

Python绘图总结(Matplotlib篇)之字体、文本及注释
https://blog.csdn.net/wuzlun/article/details/80059181

"""

import os
import sys
import datetime
from matplotlib import font_manager as fm, rcParams
import re
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import struct
import winreg
import xlwings as xw
import sqlite3

def demo():

    fig = plt.figure()
    fig.suptitle('bold figure suptitle', fontsize=14, fontweight='bold')
    
    ax = fig.add_subplot(111)
    fig.subplots_adjust(top=0.85)
    ax.set_title('axes title')
    
    ax.set_xlabel('xlabel')
    ax.set_ylabel('ylabel')
    
    ax.text(3, 8, 'boxed italics text in data coords', style='italic',
            bbox={'facecolor':'red', 'alpha':0.5, 'pad':10})
    
    ax.text(2, 6, r'an equation: $E=mc^2$', fontsize=15)
    
    ax.text(3, 2, u'unicode: Institut f\374r Festk\366rperphysik')
    
    ax.text(0.95, 0.01, 'colored text in axes coords',
            verticalalignment='bottom', horizontalalignment='right',
            transform=ax.transAxes,
            color='green', fontsize=15)
    
    
    ax.plot([2], [1], 'o')
    # 注释
    ax.annotate('我是注释啦', xy=(2, 1), xytext=(3, 4),color='r',size=15,
                arrowprops=dict(facecolor='g', shrink=0.05))
    
    ax.axis([0, 10, 0, 10])
    
    plt.show()

    return

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
def get_fqgj(gpdm,cols=None):

    data=tdxday2pd(gpdm)
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


##########################################################################
#读EXCEL文件
##########################################################################
def read_xls(xlsfn):    

    wb = xw.Book(xlsfn)

    #读取数据
    data = wb.sheets[0].range('A1').options(pd.DataFrame, expand='table').value
    wb.close()

    '''下面的语句很重要，MultiIndex转换成Index'''
    data.columns=[e[0] for e in data.columns]

    return data

##########################################################################
#获取运行程序所在驱动器
##########################################################################
def getdrive():
    if sys.argv[0]=='' :
        return os.path.splitdrive(os.getcwd())[0]
    else:
        return os.path.splitdrive(sys.argv[0])[0]

##########################################################################
#获取股东户数
##########################################################################
def get_gdhs(gpdm):
    
    gpdm=lgpdm(gpdm)
    dbfn=getdrive() + '\\hyb\\STOCKDATA.db'

    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    sql = 'SELECT rq,gdhs FROM GDHS WHERE GPDM="%s" ORDER BY RQ;' % gpdm
    curs.execute(sql)

    data = curs.fetchall()
    # 关闭游标和连接
    curs.close()
    dbcn.close()
    
    #生成有股东户数日期到现在的工作日索引
    date1=data[0][0]
    date2=datetime.datetime.now().strftime("%Y-%m-%d")
    dateidx=pd.date_range(date1,date2,freq = 'B')    
    df1=pd.DataFrame(index=dateidx)

    cols=['date','gdhs']
    df = pd.DataFrame(data,columns=cols)
    df = df.set_index('date')
    df.index = pd.DatetimeIndex(df.index)
    
    #合并生成所有工作日对应的股东户数表
    df1=df1.join(df)
    
    #用前值补缺
    df1=df1.fillna(method='ffill')
    
    
    return df1

##########################################################################
#生成大事提醒表
##########################################################################
def gendstx(dm,lxlb):
    dbfn=getdrive()+'\\hyb\\STOCKDSTX.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    
    
    sql = 'select distinct rq,ts1,ts2,tslx from dfcf where gpdm="%s" and tslx in (%s) order by rq,tslx;' % (lgpdm(dm),lxlb)
    
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
    
    df1=pd.DataFrame(rqtx,columns=['date','dstx'])

    df1=df1.set_index('date')
    df1.index = pd.DatetimeIndex(df1.index)

    return df1


if __name__ == '__main__':
    plt.rcParams['font.sans-serif']=['SimHei'] #用来正常显示中文标签
    plt.rcParams['axes.unicode_minus']=False #用来正常显示负号

    #沪深300指数
    hs300=get_fqgj('sz399300')
    hs300.columns=['hs300']
    
    gpdm='002376'
    fqgj=get_fqgj(gpdm,'volume')
    
    gdhs=get_gdhs(gpdm)
    

    if gpdm[0]=='6':
        dbzs=get_fqgj('sh000001')
        zsmc='上证指数'
    elif gpdm[:3]=='002':
        dbzs=get_fqgj('sz399005')
        zsmc='中小板指'
    elif gpdm[:3]=='300':
        dbzs=get_fqgj('sz399006')
        zsmc='创业板指'
    else:
        dbzs=get_fqgj('sz399001')
        zsmc='深证成指'

    dbzs.columns=[zsmc]
        
    lxlb='"0","1","2"'
#    lxlb='"3"'
    tsxx=gendstx(gpdm,lxlb)
#    xlsfn=r'd:\selestock\tsxx%s.xlsx' % gpdm
#    tsxx=read_xls(xlsfn)
#    tsxx.index.name='date'
#    tsxx.columns=['dstx']
    
    
#    xlsfn=r'd:\selestock\000498山东路桥.xlsx'
#    data = pd.read_excel(xlsfn)
#    data = data.set_index('date')
    '''
    Python Matplotlib简易教程
    https://blog.csdn.net/Notzuonotdied/article/details/77876080
    
    python matplotlib 图像可视化 一 (论文画图参考)
    https://www.cnblogs.com/JZ-Ser/articles/8034979.html
    
    python matplotlib 图像可视化
    
    '''
    data=fqgj.join(hs300)
    data=data.join(dbzs)
    data=data.join(gdhs)
    data=data.join(tsxx)
    
    start='2015-01-01'
    end='2018-12-31'
    data = data.loc[start:end,:]

    fig = plt.figure(figsize=(30,20))
    ax1 = plt.subplot2grid((5, 1), (0, 0), rowspan=3)
    
    ax2 = plt.subplot2grid((5, 1), (3, 0))
    ax3 = plt.subplot2grid((5, 1), (4, 0))
    ax4 = ax3.twinx()        
    ax5 = ax1.twinx()        

#    fig, (ax1,ax2) = plt.subplots(2,1,sharex='col',figsize=(30,20))
#    ax2 = ax1.twinx()
#    ax3 = ax2.twinx()
    
    
    ax1.plot(data.index,data['adj_close'],color="red",linewidth=1.5,label='adj_close')
    ax2.bar(data.index,data['volume'],color="r",linewidth=1.5,label='成交量')
    ax3.plot(data.index,data['hs300'],color="r",linewidth=1.5,label='沪深300')
    ax4.plot(data.index,data[zsmc],color="b",linewidth=1.5,label=zsmc)
    ax5.plot(data.index,data['gdhs'],color="y",linewidth=1.5,label='股东户数')
    
    title = gpdm+ "事件与股价走势图"
    fig.suptitle(title, fontsize = 14, fontweight='bold')
    
    
    ax1.set_ylabel('复权股价',color="r", fontsize = 16)
    ax2.set_ylabel('成交量',color="r", fontsize = 16)
    ax3.set_ylabel('沪深300',color="r", fontsize = 16)
    ax4.set_ylabel(zsmc,color="b", fontsize = 16)
    ax5.set_ylabel('股东户数',color="b", fontsize = 16)

    ax1.legend(loc='upper left', fontsize=16)
    ax5.legend(loc='upper right', fontsize=16)
    ax3.legend(loc='upper left', fontsize=16)
    ax4.legend(loc='upper right', fontsize=16)

    ax1.set_ylim(data['adj_close'].describe()['min']*0.8,data['adj_close'].describe()['max']*1.2)
    ax1.tick_params('y', direction='out',length=8,colors='red',pad=26)

    ax5.set_ylim(0,data['gdhs'].describe()['max']*1.1)

    ax1.grid(True,color='r',linestyle=':')
    ax2.grid(True,color='r',linestyle=':')
    ax3.grid(True,color='r',linestyle=':')
    ax4.grid(True,color='b',linestyle=':')
    
    dstx=data.dropna()
    for m in range(len(dstx)):
        x=dstx.index[m]
        y=dstx.loc[x,'adj_close']
        ts=dstx.loc[x,'dstx']+('(%s)' % x.strftime("%Y-%m-%d"))
    
        x1 = x+datetime.timedelta(4)
        if m%2==0:
            y1 = y + 1
        else:
            y1 = y - 1
            
        ax1.plot([x], [y], 'o')
        ax1.annotate(ts, xy=(x, y), xytext=(x1, y1),color='b',size=10,
                        arrowprops=dict(facecolor='g', 
                                        width=0.05, 
                                        headwidth=5, 
                                        headlength=5, 
                                        shrink=0.1))
    
    
    fig.tight_layout()
    
    imgfn = r'd:\selestock\img%s.png' % gpdm
    plt.savefig(imgfn)
    
    plt.show()
    
