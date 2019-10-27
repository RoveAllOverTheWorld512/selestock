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
import re
import matplotlib.pyplot as plt
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
def zxglst():
    zxgfile="zxg.blk"
    tdxblkdir = gettdxblkdir()
    zxgfile = os.path.join(tdxblkdir,zxgfile)
    zxg = zxglist(zxgfile,"tdxblk")
    
    gpdmb=get_gpdm()
    
    #去掉指数代码只保留A股代码
    zxglb=[]
    for e in zxg:
        dm=lgpdm(e)
        if dm in gpdmb.index:
            zxglb.append(dm)
            
    return zxglb


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
    #将字符串日期转换成pandas的日期时间
    df['rq']=pd.to_datetime(df['rq'])
    df = df.set_index('rq',drop=False)
    #
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

##########################################################################
#计算汉字串长度
##########################################################################
def strlen(s):
    l=len(s)
    utf8l=len(s.encode('utf-8'))
    return (utf8l-l)/2+l


if __name__ == '__main__':
    plt.rcParams['font.sans-serif']=['SimHei'] #用来正常显示中文标签
    plt.rcParams['axes.unicode_minus']=False #用来正常显示负号

    gpdmb=get_gpdm()

    gpdm='000069'
    gpmc=gpdmb.loc[lgpdm(gpdm)]['gpmc']
    fqgj=get_fqgj(gpdm,'volume')
    
    gdhs=get_gdhs(gpdm)
    

    #沪深300指数
    hs300=get_fqgj('sz399300')
    hs300.columns=['hs300']
    

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
    '''
    lxlb:   0、解禁
            1、大股东增减持
            2、高管增减持
            3、研报
            4、股东户数增减
            5、回购
            6、公告
    '''
        
    lxlb='"0","1","2","4","5","6"'
#    lxlb='"0","1","2","3","4","5","6"'
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
    
    绘图: matplotlib核心剖析
    http://www.cnblogs.com/vamei/archive/2013/01/30/2879700.html
    
    Python绘图总结(Matplotlib篇)之坐标轴及刻度
    https://blog.csdn.net/wuzlun/article/details/80053277
    
    	
    matplotlib x轴时间显示
    https://blog.csdn.net/ying_593254979/article/details/17451661
    
    一份非常好的Matplotlib 教程
    https://blog.csdn.net/u011497262/article/details/52325705
    '''
    data=fqgj.join(gdhs)    
    
    start='2017-01-01'
    end='2018-12-31'
    data = data.loc[start:end,:]
    hs300=hs300.loc[start:end,:]
    dbzs=dbzs.loc[start:end,:]

    ymin=data['adj_close'].describe()['min']*0.8
    ymax=data['adj_close'].describe()['max']*1.2

    xmin=plt.matplotlib.dates.date2num(datetime.datetime(2017, 1, 1, 0, 0))
    xmax=plt.matplotlib.dates.date2num(datetime.datetime(2018, 12, 31, 0, 0))
    
    '''
    对png图片进行测量，在72dpi下ax1的Y轴坐标（84，844），（84，19）长度为826像素
    12号字体为12*12点阵，如果保证行间有1个像素，826/13=63.5,也就是说可以显示60行
    不会重叠。这样可以算出1行对应y值的值,2080/12
    '''
    
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
    ax3.plot(hs300.index,hs300['hs300'],color="r",linewidth=1.5,label='沪深300')
    ax4.plot(dbzs.index,dbzs[zsmc],color="b",linewidth=1.5,label=zsmc)
    ax5.plot(data.index,data['gdhs'],color="y",linewidth=1.5,label='股东户数')
    
    title = gpdm+gpmc+ "事件与股价走势图"
    fig.suptitle(title, fontsize = 20, fontweight='bold')
    

    ax1.set_ylim(ymin,ymax)
    ax1.set_xlim(xmin,xmax)
    ax2.set_xlim(xmin,xmax)
    ax3.set_xlim(xmin,xmax)
    
    ax1.set_ylabel('复权股价',color="r", fontsize = 16)    #在png图上是14*14像素的字

    ax2.set_xlim(xmin,xmax)
    ax2.set_ylabel('成交量',color="r", fontsize = 16)
    ax3.set_ylabel('沪深300',color="r", fontsize = 16)
    ax4.set_ylabel(zsmc,color="b", fontsize = 16)
    ax5.set_ylabel('股东户数',color="b", fontsize = 16)

    ax1.legend(loc='upper left', fontsize=16)
    ax5.legend(loc='upper right', fontsize=16)
    ax3.legend(loc='upper left', fontsize=16)
    ax4.legend(loc='upper right', fontsize=16)

    ax1.tick_params('y', direction='out',length=8,colors='red',pad=26)

    ax5.set_ylim(0,data['gdhs'].describe()['max']*1.1)

    ax1.grid(True,color='r',linestyle=':')
    ax2.grid(True,color='r',linestyle=':')
    ax3.grid(True,color='r',linestyle=':')
    ax4.grid(True,color='b',linestyle=':')

    endtime=datetime.datetime.strptime('2018-07-30',"%Y-%m-%d")
    hm = 50     #ax1最大行数
    zm = 290     #ax1一行显示最多字数 
    yfont12=(ymax-ymin)/hm      #按价格计算一行的高度
#    xmin,xmax=ax1.get_xlim()        

    '''
    xdatemin=plt.matplotlib.dates.num2date(xmin)
    xdatemax=plt.matplotlib.dates.num2date(xmax)
    xmin=plt.matplotlib.dates.date2num(datetime.datetime(2017, 1, 1, 0, 0))
    xmax=plt.matplotlib.dates.date2num(datetime.datetime(2018, 12, 31, 0, 0))
    '''
    
    xfont12=(xmax-xmin)/zm      #一个英文字符所在日数长度
    
    #由于有些研报、公告等信息公布的日期不在交易时间，所以采用全连接，补齐这些日期
    #再用前收盘价补齐缺失值    
    data1=data.join(tsxx,how='outer')
    data1=data1[['adj_close']]
    data1=data1.fillna(method='ffill')

    dstx=data1.join(tsxx)    
    dstx=dstx.dropna()
    dstx=dstx.assign(y1=dstx['adj_close'].map(lambda x:int((x-ymin)/yfont12)))
    dstx=dstx.assign(y0=dstx['y1'])
    dstx=dstx.assign(l=dstx['dstx'].map(strlen))
    dstx=dstx.assign(x0=dstx.index)
    dstx=dstx.assign(x1=dstx.index)
    for rq in dstx.index:
        x0=dstx.loc[rq]['x0']
        x1=x0+datetime.timedelta(int(dstx.loc[rq]['l']*xfont12+16))
#        if x1>endtime:
#            qy=x1-endtime      #前移量
#            x0=x0-qy
#            x1=x1-qy
#            print(rq,x0,x1)
            
#        dstx.loc[rq,'x0']=x0
        dstx.loc[rq,'x1']=x1
        
    hdict={}
    for i in range(len(dstx)):
        rq = dstx.index[i]
        h=dstx.iloc[i]['y1']
        x0=dstx.iloc[i]['x0']
        x1=dstx.iloc[i]['x1']

        hh=[0]
        for j in range(1,20):
            if h-j>0:
                hh.append(-j)
            if h+j<hm:
                hh.append(j)

        if h not in hdict.keys():
            hdict[h]=[[x0,x1]]
        else:
            for j in hh:
                h0=h+j
                if h0 in hdict.keys():
                    bzqj=hdict[h0]          #被占区间
                    cd=False                #重叠标记
                    for rq1,rq2 in bzqj:
                        if (x0>=rq1 and x0<=rq2) or (x1>=rq1 and x1<=rq2):
                            cd=True         #本行对应区域已被占用
                            break
                    if cd:
                        continue
                    else:
                        dstx.loc[rq,'y1']=h0
                        bzqj.append([x0,x1])
                        hdict[h0]=bzqj
                        break
                else:
                    dstx.loc[rq,'y1']=h0
                    hdict[h0]=[[x0,x1]]
                    break
                
                

    for m in range(len(dstx)):
        x=dstx.index[m]
        y=dstx.loc[x,'adj_close']
        h=dstx.loc[x,'y1']
        ts=dstx.loc[x,'dstx']+('(%s)' % x.strftime("%Y-%m-%d"))
        
        x1 = x+datetime.timedelta(10)

        y1 = ymin +(h-3)*yfont12   
         
        ax1.plot([x], [y], 'o')
        ax1.annotate(ts, xy=(x, y), xytext=(x1, y1),color='b',size=12,
                        arrowprops=dict(facecolor='g', 
                                        width=0.05, 
                                        headwidth=5, 
                                        headlength=5
                                        ))
    
        #标注字在png图上是12*12x像素的字体
    fig.tight_layout()
    now=datetime.datetime.now().strftime('_%Y%m%d_%H%M')
    imgfn = r'd:\selestock\img%s%s.png' % (gpdm,now)
    plt.savefig(imgfn)
    
    plt.show()
    
