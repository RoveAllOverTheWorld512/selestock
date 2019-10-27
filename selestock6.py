# -*- coding: utf-8 -*-
"""


常用回测评判指标解读
https://uqer.datayes.com/community/share/56a60b8a228e5b2044d915ca

单一股票质押比例（中国结算每周公布）
http://www.chinaclear.cn/cms-rank/downloadFile?queryDate=2018.06.08&type=proportion

通达信行业、板块与自定义指数
http://blog.sina.com.cn/s/blog_623d2d280102vt8y.html
https://blog.csdn.net/liuyukuan/article/details/79483812

python 适用通达信
http://www.360doc.com/content/17/0523/19/8392_656548117.shtml

[python]解析通达信盘后数据获取历史日线数据
https://blog.csdn.net/liuyukuan/article/details/53560278


http://blog.sina.com.cn/s/blog_154861eae0102xcsk.html
"""

import os
import sys
import re
import datetime
import dateutil.parser
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
import requests
import zipfile
from bs4 import BeautifulSoup as bs
import dateutil.parser
import xlrd
import subprocess
from io import StringIO

########################################################################
#建立数据库
########################################################################
def createDataBase():
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    cn = sqlite3.connect(dbfn)

    """
    股票代码表：股票代码，股票名称
    """

    cn.execute('''CREATE TABLE IF NOT EXISTS GPDM
           (GPDM TEXT PRIMARY KEY,
           GPMC TEXT);''')

    """
    股票EPS：股票代码，日期，基本EPS0，稀释EPS1，基本EPS0同比增长率，稀释EPS1同比增长率
    """

    cn.execute('''CREATE TABLE IF NOT EXISTS GPEPS
           (GPDM TEXT,
           RQ TEXT,
           EPS0 REAL,
           EPS1 REAL,
           EPS0_G REAL,
           EPS1_G REAL);''')

    cn.execute('''CREATE UNIQUE INDEX IF NOT EXISTS GPEPS_GPDM_RQ ON GPEPS(GPDM,RQ);''')
 
    """
    股票成长性：股票代码，日期，营业总收入同比增长率(%)，营业收入同比增长率(%)，净利润同比增长率(%)，
    扣除非经常性损益后的净利润同比增长率(%)，营业收入(元)，净利润(元)，非经常性损益(元)，上年同期净利润(元)
    
    """

    cn.execute('''CREATE TABLE IF NOT EXISTS GPGROWTH
           (GPDM TEXT,
           RQ TEXT,
           YYZSR_G REAL,
           YYSR_G REAL,
           JLR_G REAL,
           KFJLR_G REAL,
           YYSR REAL,
           JLR REAL,
           FSY REAL,
           SNJLR REAL);''')

    cn.execute('''CREATE UNIQUE INDEX IF NOT EXISTS GPGROWTH_GPDM_RQ ON GPGROWTH(GPDM,RQ);''')

    """
    股票年报关键数据：股票代码，日期，经营现金流量净额，营业收入，净利润，资产总计，流动资产，净资产，
    商誉，带息债务，应收账款，加权净资产收益率，杠杆倍数，带息负债率，商誉净资产占比，商誉总资产占比，
    收入经营现金含量，净利润经营现金含量，应收账款收入占比
    算法：
    杠杆倍数=净资产/资产总计
    带息负债率=带息债务/资产总计
    商誉净资产占比=商誉/净资产
    商誉总资产占比=商誉/总资产
    收入经营现金含量=经营现金流量净额/营业收入
    净利润经营现金含量=经营现金流量净额/净利润
    应收账款收入占比=应收账款/营业收入
    """

    cn.execute('''CREATE TABLE IF NOT EXISTS GPNB
           (GPDM TEXT,
           RQ TEXT,
           JYXJL REAL,
           YYSR REAL,
           JLR REAL,
           ZCZJ REAL,
           LDZC REAL,
           JZC REAL,
           SY REAL,
           DXZW REAL,
           YSZK REAL,
           ROE REAL,
           GGBS REAL,
           DXFZL REAL,
           SYJZCZB REAL,
           SYZZCZB REAL,
           SRXJLHL REAL,
           LRXJLHL REAL,
           YSZKSRZB REAL);''')

    cn.execute('''CREATE UNIQUE INDEX IF NOT EXISTS GPNB_GPDM_RQ ON GPNB(GPDM,RQ);''')

    cn.commit()

########################################################################
#初始化本程序配置文件
########################################################################
def iniconfig(inifile=None):
    if inifile==None:
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

###############################################################################
#检测股票代码为A股代码
###############################################################################
def agu_yes(gpdm):
    gpdm=sgpdm(gpdm)
    return gpdm!=None and (gpdm[0] =='6' or gpdm[:2]=='00' or gpdm[:3] in ('002','300'))
        

#############################################################################
#读取dbf文件
#############################################################################
def dbfreader(f):

    numrec, lenheader = struct.unpack('<xxxxLH22x', f.read(32))
    numfields = (lenheader - 33) // 32

    fields = []
    for fieldno in range(numfields):
        name, typ, size, deci = struct.unpack('<11sc4xBB14x', f.read(32))
        name = name.decode().replace('\x00', '')
        typ  = typ.decode()
        fields.append((name, typ, size, deci))
    yield [field[0] for field in fields]
    yield [tuple(field[1:]) for field in fields]

    f.read(1) #读取扔掉

    fields.insert(0, ('DeletionFlag', 'C', 1, 0))
    fmt = ''.join(['%ds' % fieldinfo[2] for fieldinfo in fields])
    fmtsiz = struct.calcsize(fmt)

    for i in range(numrec):
        record = struct.unpack(fmt, f.read(fmtsiz))
        if record[0].decode() != ' ':
            continue                        # deleted record
        result = []
        for (name, typ, size, deci), value in list(zip(fields, record)):
            if name == 'DeletionFlag':
                continue
            if typ == "C":
                value = value.strip(b'\x00').decode('GBK')
            if typ == "N":
                value = value.strip(b'\x00').strip(b'\x20').decode('GBK')
                if value == '':
                    value = 0
                elif deci:
                    value = float(value)
                else:
                    value = int(value)
            elif typ == 'D':
                value = value.decode('GBK')
            elif typ == 'L':
                value = value.decode('GBK')
                value = (value in 'YyTt' and 'T') or (value in 'NnFf' and 'F')

            result.append(value)

        yield result

###############################################################################
#将通达信.day读入pands
###############################################################################
def day2pd(dayfn,start=None,end=None):
    
    if end == None:
        end=datetime.datetime.now().strftime('%Y%m%d')
    if start == None:
        start='19900101'

    records = []
    columns = ['rq','date','open', 'high', 'low','close','amout','volume','rate','pre_close','adj_rate','adj_close']

    with open(dayfn,"rb") as f:
        data = f.read()
        f.close()
    days = int(len(data)/32)

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
    
    df = df.set_index('rq')     #索引会自动转换为日期时间索引类型DatetimeIndex
    '''
    
    df.index=pd.to_datetime(df['date'])
    '''
    start = str2datetime(start)
    end = str2datetime(end)

    if start == None or end==None :
        return df
    else :
        return df[start:end]

        
###############################################################################
#将通达信.day读入pands
###############################################################################
def tdxday2pd(gpdm,start=None,end=None):
    
    gpdm=sgpdm(gpdm)

    sc = 'sh' if gpdm[0]=='6' else 'sz'
    dayfn =getdrive()+'\\tdx\\'+sc+'lday\\'+sc+gpdm+'.day'
#    dayfn =gettdxdir()+'\\vipdoc\\'+sc+'\\lday\\'+sc+gpdm+'.day'

    if os.path.exists(dayfn) :
        df = day2pd(dayfn,start,end)
    else :
        records = []
        columns = ['rq','date','open', 'high', 'low','close','amout','volume','rate','pre_close','adj_rate','adj_close']
        df = pd.DataFrame(records,columns=columns)

    return df

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
    df=pd.DataFrame(datacode,columns=['gpdm','gpmc','gppy'])
    df['dm']=df['gpdm'].map(lambda x:x[:6])
    df=df.set_index('gpdm',drop=False)
    return df

###############################################################################
#从通达信系统读取股票上市日期
###############################################################################
def get_ssrq():
    fn=gettdxdir()+"\\T0002\\hq_cache\\base.dbf"
    ssrq = dbf2pandas(fn,['sc','gpdm', 'ssdate']) 
    
    gpssb={}
    for index,row in ssrq.iterrows():
        sc=row['sc']
        gpdm=row['gpdm']
        
        if len(row['ssdate'].strip())==8 :
            
            if (sc=='1' and gpdm[0]=='6') :
                gpssb[gpdm+'.SH'] = row['ssdate']
            if (sc=='0' and (gpdm[:2]=='00' or gpdm[:2]=='30')) :
                gpssb[gpdm+'.SZ'] = row['ssdate']
            
    gpssrq=pd.DataFrame({'ssrq':gpssb})
    gpssrq.index.name='gpdm'

    return gpssrq

###############################################################################
#读取dbf到pandas
###############################################################################
def dbf2pandas(dbffn,cols):
    with open(dbffn,"rb") as f:
        data = list(dbfreader(f))
        f.close()
    columns = data[0]
    columns=[e.lower() for e in columns]
    data = data[2:]
    df = pd.DataFrame(data,columns=columns)
    if len(cols) == 0 :
        return df
    else :
        return df[cols]

########################################################################
#创建目录
########################################################################
def makedir(dirname):
    if dirname == None :
        return False

    if not os.path.exists(dirname):
        try :
            os.mkdir(dirname)
            return True
        except(OSError):
            print("创建目录%s出错，请检查！" % dirname)
            return False
    else :
        return True

##########################################################################
#n天后日期串，不成功返回None
##########################################################################
def nextdtstr(s,n):
    dt = str2datetime(s)
    if dt :
        dt += datetime.timedelta(n)
        return dt.strftime("%Y%m%d")
    else :
        return None


###############################################################################
#获取最新交易日，如果当天是交易日，在16:00后用当天
#tushare/stock/cons.py                
#calAll.csv从http://file.tushare.org/tsdata/calAll.csv下载     
#d:\hyb\A_exchange_cal.py也可以生成该文件                
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
    #不是交易日或者是交易日但在16:00点以前，则取前一个交易日
    if td!=days[i] or now.strftime("%H%M")<'1600':
        i = i-1

    return days[i]    

###############################################################################
#获取指定日期前n个交易日的日期,如果指定的日期不是交易日，则取该日期前最近的一个交易日作为起始
#
###############################################################################
def openday_list():
    df = pd.read_csv(r'd:\selestock\calAll.csv', dtype={'calendarDate':'object'})
    days=df.loc[df['isOpen']==1,'calendarDate']
    days=[e.replace('-','') for e in days]
    days.sort()
    return days

###############################################################################
#获取指定日期前n个交易日的日期,如果指定的日期不是交易日，则取该日期前最近的一个交易日作为起始
#odays为开市日期列表
###############################################################################
def get_openday_pre(dt,n):

    dt=dateutil.parser.parse(dt).strftime("%Y%m%d")

    now = datetime.datetime.now()
    td = now.strftime("%Y%m%d") #今天
    #如果指定的日期在今天之后则为今天
    dt=dt if dt<td else td
    
    i=0
    while True :
        if dt<=odays[i]:
            break
        else:
            i += 1
    #不是交易日，则取前一个交易日
    if dt!=odays[i]:
        i = i-1
        
    if i-n>=0:
        pre_dt=odays[i-n]
    else:
        pre_dt=odays[0]
        
    return dateutil.parser.parse(pre_dt).strftime("%Y-%m-%d")

###############################################################################
#
###############################################################################

'''
 urllib.urlretrieve 的回调函数：
def callbackfunc(blocknum, blocksize, totalsize):
    @blocknum:  已经下载的数据块
    @blocksize: 数据块的大小
    @totalsize: 远程文件的大小
'''
 
def Schedule(blocknum, blocksize, totalsize):

    global start_time

    n = 50
    blk = int((totalsize / blocksize + (n-1))/n)
    blkn=[]
    for i in range(n):
        blkn.append(i*blk)
    
    if blocknum in blkn:   
        if blocknum==0:
            print('\n')
            
        recv_size = blocknum * blocksize
        speed = recv_size / (time.time() - start_time)
        # speed_str = " Speed: %.2f" % speed
        speed_str = " Speed: %s" % format_size(speed)
         
        # 设置下载进度条
    
        pervent = recv_size / totalsize
        
        percent_str = "%.2f%%" % (pervent * 100)
        
        
        n = round(pervent * 50)
        s = ('#' * n).ljust(50, '-')
        print(percent_str.ljust(8, ' ') + '[' + s + ']' + speed_str) 
        
    if blocknum >= totalsize/blocksize:
        print("100.00% "+"["+"#"*50+"] OK")

########################################################################
# 字节bytes转化K\M\G
########################################################################
def format_size(bytes):
    try:
        bytes = float(bytes)
        kb = bytes / 1024
    except:
        print("传入的字节格式不对")
        return "Error"
    if kb >= 1024:
        M = kb / 1024
        if M >= 1024:
            G = M / 1024
            return "%.3fG" % (G)
        else:
            return "%.3fM" % (M)
    else:
        return "%.3fK" % (kb)


########################################################################
#获取通达信网站day.zip文件时间戳
########################################################################
def dayfile_update():
    
    fireFoxOptions = webdriver.FirefoxOptions()

    fireFoxOptions.headless=True

    browser = webdriver.Firefox(options=fireFoxOptions)

    url='http://www.tdx.com.cn/list_66_69.html'    
    browser.get(url)

    tbl = WebDriverWait(browser, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "tbody")))
        
    html=tbl.get_attribute('innerHTML')
    
    browser.quit()
    
    rows=pq(html)
    shlday=rows('span#td004').text()
    szlday=rows('span#td005').text()

    timeArray = time.strptime(shlday, "%Y/%m/%d %H:%M:%S")
    shlday=time.mktime(timeArray)

    timeArray = time.strptime(szlday, "%Y/%m/%d %H:%M:%S")
    szlday=time.mktime(timeArray)

    return {'shlday.zip':shlday,'szlday.zip':szlday}


########################################################################
#获取通达信网站day.zip文件时间戳
########################################################################
def dayfileupdate():

    url='https://www.tdx.com.cn/products/getfileinfo_sysj.asp?fpath=./data/data/vipdoc/shlday.zip&t=1552549223124'
    txt=requests.get(url).text
    mat = re.search(r"(\d{4}/\d{1,2}/\d{1,2} \d{2}:\d{2}:\d{2})",txt)
    txt=mat.groups(0)[0]
    timeArray = time.strptime(txt, "%Y/%m/%d %H:%M:%S")
    shlday=time.mktime(timeArray)

    url='https://www.tdx.com.cn/products/getfileinfo_sysj.asp?fpath=./data/data/vipdoc/szlday.zip&t=1552549835438'
    txt=requests.get(url).text
    mat = re.search(r"(\d{4}/\d{1,2}/\d{1,2} \d{2}:\d{2}:\d{2})",txt)
    txt=mat.groups(0)[0]
    timeArray = time.strptime(txt, "%Y/%m/%d %H:%M:%S")
    szlday=time.mktime(timeArray)

    return {'shlday.zip':shlday,'szlday.zip':szlday}


########################################################################
#获取本机通达信安装目录，生成自定义板块保存目录
########################################################################
def dlday0():
    global start_time
    '''
    #每天下载一次
    http://www.tdx.com.cn/list_66_69.html
    http://www.tdx.com.cn/products/data/data/vipdoc/shlday.zip
    http://www.tdx.com.cn/products/data/data/vipdoc/szlday.zip
    '''

    #如果zfdf.csv文件存在且是最新交易日16：00以后生成的，则不用下载数据，计算涨幅    
    zfdffn=samepath()+'\\zfdf.csv'
    if os.path.exists(zfdffn) :
        mtime=os.path.getmtime(zfdffn)
        ltime=time.strftime("%Y%m%d%H%M",time.localtime(mtime))
        lstd=lastopenday()
        if ltime>lstd+'1600' :
            return

    #查看数据文件的更新时间
    dayupdate=dayfileupdate()
    calc_zfdf=False
    
    url0 = "http://www.tdx.com.cn/products/data/data/vipdoc/"
    fnls = ["shlday.zip","szlday.zip"]
    svdir = getdrive()+"\\tdx"
    if not os.path.exists(svdir) :
        makedir(svdir)

    for fn in fnls:
        start_time = time.time()
#        print(start_time)

        dlyes = False    #下载标志，True表示要下载
        zip_file = svdir + "\\" + fn
        url = url0 + fn
        if os.path.exists(zip_file):
            mtime=os.path.getmtime(zip_file)  #文件建立时间戳
            dlyes = (mtime < dayupdate[fn])
        else :
            dlyes = True


        if dlyes:
            print ("正在下载的文件%s，请等待！" % zip_file)

            if os.path.exists(zip_file):
                os.remove(zip_file)
            
            request.urlretrieve(url, zip_file, Schedule)

        if dlyes and os.path.exists(zip_file):
            print ("正在解压文件%s，请等待！" % zip_file)
            extdir = svdir + "\\" + fn[:6]
            f_zip = zipfile.ZipFile(zip_file, 'r')
            f_zip.extractall(extdir)
            f_zip.close()

            calc_zfdf=True
    
    if calc_zfdf:
        zfhc()

    return

########################################################################
#获取本机通达信安装目录，生成自定义板块保存目录
########################################################################
def dlday():
    global start_time,calc_zfdf
    '''
    #每天下载一次
    http://www.tdx.com.cn/list_66_69.html
    http://www.tdx.com.cn/products/data/data/vipdoc/shlday.zip
    http://www.tdx.com.cn/products/data/data/vipdoc/szlday.zip
    '''

    #如果zfdf.csv文件存在且是最新交易日16：00以后生成的，则不用下载数据，计算涨幅    
    zfdffn=samepath()+'\\zfdf.csv'
    if os.path.exists(zfdffn) :
        mtime=os.path.getmtime(zfdffn)
        ltime=time.strftime("%Y%m%d%H%M",time.localtime(mtime))
        lstd=lastopenday()
        if ltime>lstd+'1600' :
            calc_zfdf = False
            return

    #查看数据文件的更新时间
    dayupdate=dayfileupdate()
    
    url0 = "http://www.tdx.com.cn/products/data/data/vipdoc/"
    fnls = ["shlday.zip","szlday.zip"]
    svdir = getdrive()+"\\tdx"
    if not os.path.exists(svdir) :
        makedir(svdir)

    for fn in fnls:
        start_time = time.time()
#        print(start_time)

        dlyes = False    #下载标志，True表示要下载
        zip_file = svdir + "\\" + fn
        url = url0 + fn
        if os.path.exists(zip_file):
            mtime=os.path.getmtime(zip_file)  #文件建立时间戳
            dlyes = (mtime < dayupdate[fn])
        else :
            dlyes = True


        if dlyes:
            print ("正在下载的文件%s，请等待！" % zip_file)

            if os.path.exists(zip_file):
                os.remove(zip_file)
            
            request.urlretrieve(url, zip_file, Schedule)

    fnls = [["shlday.zip",'sh000001.day'],["szlday.zip",'sz399001.day']]
    for fn,fnd in fnls:
        dayfn = svdir + "\\" + fn[:6] + '\\' + fnd
        daymtime=os.path.getmtime(dayfn)  #文件建立时间戳

#        print(dayfn,daymtime) 
        
        zip_file = svdir + "\\" + fn
        zipmtime=os.path.getmtime(zip_file)  #文件建立时间戳

#        print(zip_file,zipmtime) 

        if (daymtime<zipmtime) and os.path.exists(zip_file):
            print ("正在解压文件%s，请等待！" % zip_file)
            extdir = svdir + "\\" + fn[:6]
            f_zip = zipfile.ZipFile(zip_file, 'r')
            f_zip.extractall(extdir)
            f_zip.close()

    calc_zfdf=True
    
    return


########################################################################
#获取本机通达信安装目录，生成自定义板块保存目录
########################################################################
def dlday1():
    global start_time
    '''
    #每天下载一次
    http://www.tdx.com.cn/list_66_69.html
    http://www.tdx.com.cn/products/data/data/vipdoc/shlday.zip
    http://www.tdx.com.cn/products/data/data/vipdoc/szlday.zip
    '''

    url0 = "http://www.tdx.com.cn/products/data/data/vipdoc/"
    fnls = ["shlday.zip","szlday.zip"]
    svdir = getdrive()+"\\tdx"
    if not os.path.exists(svdir) :
        makedir(svdir)

    for fn in fnls:
        start_time = time.time()
#        print(start_time)

        dlyes = False    #下载标志，True表示要下载
        zip_file = svdir + "\\" + fn
        url = url0 + fn
        if os.path.exists(zip_file):
            
            mtime=os.path.getmtime(zip_file)  #文件建立时间
            
            ltime=time.strftime("%Y%m%d",time.localtime(mtime))
            if ltime >= lastopenday() :
                #必须是最后一个交易日16点以后下载的最新数据
                if ltime == lastopenday() and time.strftime("%H",time.localtime(mtime))<'16':
                    dlyes = True
                else:    
                    dlyes = False
            else :
                dlyes = True
        else :
            dlyes = True


        if dlyes:
            print ("正在下载的文件%s，请等待！" % zip_file)

            if os.path.exists(zip_file):
                os.remove(zip_file)
            
            request.urlretrieve(url, zip_file, Schedule)

        if dlyes and os.path.exists(zip_file):
            print ("正在解压文件%s，请等待！" % zip_file)
            extdir = svdir + "\\" + fn[:6]
            f_zip = zipfile.ZipFile(zip_file, 'r')
            f_zip.extractall(extdir)
            f_zip.close()


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
#获取中证指数公司公布的PE、PB
########################################################################
def get_pepb(date):
    dbfn=getdrive()+'\\hyb\\STOCKDATA.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    
#    sql = 'select gpdm,pe_lyr,pe_ttm,pb from pe_pb where rq="'+date+'";'
#    curs.execute(sql)
    
    curs.execute('select gpdm,pe_lyr,pe_ttm,pb from pe_pb where rq=?',(date,))
    data = curs.fetchall()
    cols = ['gpdm','pe_lyr','pe_ttm','pb']
    
    df = pd.DataFrame(data,columns=cols)

    dbcn.close()    

    return df.set_index('gpdm')
    
###############################################################################
#下载文件名，参数1表示如果文件存在则将原有文件名用其创建时间命名
###############################################################################
def dlfn(dldir):

    today=datetime.datetime.now().strftime("%Y-%m-%d")
    dlfn = today+'.xls'
    fn = os.path.join(dldir,dlfn)

    if os.path.exists(fn):
        ctime=os.path.getctime(fn)  #文件建立时间
        ltime=time.localtime(ctime)
        newfn = time.strftime("%Y%m%d%H%M%S",ltime)+'.xls'
        os.rename(fn,os.path.join(os.path.dirname(fn),newfn))

    return fn

###############################################################################
#从同花顺i问财下载
###############################################################################
def dl_ths_xls(prefix):

    config = iniconfig()
    
    ddir=os.path.join(getdrive(),readkey(config,'dldir'))
    
    dafn = dlfn(ddir)   #如果文件已存在则修改文件名

    username = readkey(config,'iwencaiusername')
    pwd = readkey(config,'iwencaipwd')
    
    profile = webdriver.FirefoxProfile()
    profile.set_preference('browser.download.dir', ddir)
    profile.set_preference('browser.download.folderList', 2)
    profile.set_preference('browser.download.manager.showWhenStarting', False)
    
    #http://www.w3school.com.cn/media/media_mimeref.asp
    profile.set_preference('browser.helperApps.neverAsk.saveToDisk', 'application/vnd.ms-excel')
    
    browser = webdriver.Firefox(firefox_profile=profile)

    #浏览器窗口最大化
    browser.maximize_window()
    #登录同花顺
    browser.get("http://upass.10jqka.com.cn/login")
    #time.sleep(1)
    elem = browser.find_element_by_id("username")
    elem.clear()
    elem.send_keys(username)
    
    elem = browser.find_element_by_class_name("pwd")
    elem.clear()
    elem.send_keys(pwd)
    
    browser.find_element_by_id("loginBtn").click()
    time.sleep(2)

    if isinstance(prefix,str):
        kw = readkey(config, prefix + 'kw')
        sl = readkey(config, prefix + 'sl')
    
        newfn = readkey(config, prefix + 'fn')
        newfn = os.path.join(ddir,newfn)
        if os.path.exists(newfn):
            os.remove(newfn)

        dlthsxls(browser,kw,sl)
        if os.path.exists(dafn):
            os.rename(dafn,newfn)

    elif isinstance(prefix,list) or isinstance(prefix,tuple):
        for pre in prefix:
            kw = readkey(config, pre + 'kw')
            sl = readkey(config, pre + 'sl')
        
            newfn = readkey(config, pre + 'fn')
            newfn = os.path.join(ddir,newfn)
            if os.path.exists(newfn):
                os.remove(newfn)
    
            dlthsxls(browser,kw,sl)
            if os.path.exists(dafn):
                os.rename(dafn,newfn)
           
            
    browser.quit()


#########################################################################
#从同花顺i问财下载
#########################################################################
def dlthsxls(browser,kw,sl):
    browser.get("http://www.iwencai.com/")
    time.sleep(3)

    browser.find_element_by_id("auto").clear()
    browser.find_element_by_id("auto").send_keys(kw)
    browser.find_element_by_id("qs-enter").click()
    time.sleep(5)
    
    #打开查询项目选单
    trigger = browser.find_element_by_class_name("showListTrigger")
    trigger.click()
    time.sleep(3)
    
    #获取查询项目选单
    checkboxes = browser.find_elements_by_class_name("showListCheckbox")
    indexstrs = browser.find_elements_by_class_name("index_str")
    
    #去掉选项前的“√”
    #涨幅、股价保留
    for i in range(0,len(checkboxes)):
        checkbox=checkboxes[i]
        
        #对于“pe,ttm”之类中间的逗号用下划线代替，注意配置文件也需要这样
        indexstr=indexstrs[i].text.replace(",","_")
        
        if checkbox.is_selected() and not indexstr in sl :
            checkbox.click()
        if not checkbox.is_selected() and indexstr in sl :
            checkbox.click()

    #向上滚屏
    js="var q=document.documentElement.scrollTop=0"  
    browser.execute_script(js)  
    time.sleep(1) 
    
    #关闭查询项目选单
    trigger = browser.find_element_by_class_name("showListTrigger")
    trigger.click()
    time.sleep(1)
    
    #导出数据
    elem = browser.find_element_by_class_name("export.actionBtn.do") 
    #在html中类名包含空格
    elem.click() 

    time.sleep(30)     #此句很重要，没有就不能保存下载的文件

    return True


#########################################################################
#将字符串转换成数值
#########################################################################
def str2float_none(x):
    try:
        return float(x)
    except:
        return None

#########################################################################
#读取读取i问财下载的xls文件
#########################################################################
def xls2pd(xlsfn,coldic,colfloat,colsele):

    '''
    读取i问财下载的xls文件
    
    案例：
        xlsfn = 'd:\\selestock\\gdzjc.xls'
        coldic={'股票代码':'gpdm',	
            '股票简称':'gpmc',	
            '变动日期'	:'bdrq',
            '大股东变动股数(股)':'bdgs',	
            '变动数量占流通股比(%)':'bdgszb',	
        
            '变动均价(元)':'bgjj',	
            '变动市值(元)':'bdsz',	
            '大股东增减持方向':'zjcfx',	
            '增减持股东名称':'gdmc',	
            '增减持股东类型':'gdlx',	
        
            '是否在高位增减持':'sfgwzjc',	
            '股东性质':'gdxz',	
            '股东增减持方式':'zjcfs',	
            '大股东增减持公告日期':'ggrq'
            }
        
        colfloat=['bdgs','bdgszb','bgjj','bdsz']
        colsele=[coldic[e] for e in coldic]
    
    '''
    with open(xlsfn,'rb') as f:
        content = f.read().decode('utf8','ignore')
        f.close()

    soup = bs(content,"lxml")
    tables = soup.findAll('table')
    tab = tables[0]
    data=[]
    for tr in tab.findAll('tr'):
        row=[]
        tds=tr.findAll('td')
        for i in range(len(tds)):
            content=tds[i].getText().replace('\t','').replace('\r','').replace('--','')
            if len(content)==0:
                content=None
            row.append(content)
        data.append(row)

    cols=data[0]
    #下面是去掉列名中的日期
    for i in range(len(cols)):
        cols[i] = re.sub('\d{4}\.\d{2}\.\d{2}','',cols[i])
        cols[i] = re.sub('-','',cols[i])
        cols[i].replace(' ','')
    
    data=data[1:]
    df = pd.DataFrame(data,columns=cols)

    flds=[]
    df.columns   
    for i in range(len(df.columns)):
        fld=df.columns[i]
        if fld in coldic.keys():
            flds.append(coldic[fld])
        else:
            flds.append(fld)
    
    df.columns=flds   
    #提取有用字段
    df=df[colsele]     
    
    for fld in colfloat:
        df[fld]=df[fld].map(str2float_none)
        
        
        
    return df

##########################################################################
#获取运行程序所在驱动器
##########################################################################
def getpath():
    return os.path.dirname(sys.argv[0])


#############################################################################
#获取市盈率文件交易日列表
#############################################################################
def jyrlist(syldir):
    files = os.listdir(syldir)
    fs = [re.findall('csi(\d{8})\.xls',e) for e in files]
    jyrqlist =[]
    for e in fs:
        if len(e)>0:
            jyrqlist.append(e[0])

    return sorted(jyrqlist,reverse=1)


##########################################################################
#获取运行程序所在驱动器
##########################################################################
def getdrive():
    if sys.argv[0]=='' :
        return os.path.splitdrive(os.getcwd())[0]
    else:
        return os.path.splitdrive(sys.argv[0])[0]


##########################################################################
#删除所有表
##########################################################################
def DeleTables(dbfn):    
#    dbfn=getdrive()+'\\hyb\\STOCKHY.db'
    cn = sqlite3.connect(dbfn)
    curs = cn.cursor()

    curs.execute('''SELECT name FROM sqlite_master WHERE type ='table' 
                 AND name != 'sqlite_sequence';''')
    
    data = curs.fetchall()
    for tbn in data:
        print("DROP TABLE "+ tbn[0])
        curs.execute("DROP TABLE "+ tbn[0])

    cn.commit()
    cn.close()
        
##########################################################################
#查询股票
##########################################################################
def Query(gpdm):
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    cn = sqlite3.connect(dbfn)
    curs = cn.cursor()
    sql='''select gpfldm.gpdm,gpdm.gpmc,gpfldm.zldm,zldm.ZLMC,gpfldm.fldm,fldm.flmc 
            from gpfldm,gpdm,zldm,fldm where gpfldm.gpdm=="'''+gpdm.upper()+'''" 
            and gpfldm.gpdm==gpdm.gpdm and gpfldm.ZLDM==zldm.ZLDM 
            and gpfldm.fldm=fldm.fldm and fldm.zldm=zldm.zldm;'''
    curs.execute(sql)        
    data = curs.fetchall()
    
    return data

##########################################################################
#股票代码表
##########################################################################
def gpdmtbl():
    
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    cn = sqlite3.connect(dbfn)
    gpdmb=get_gpdm()
    data=[[a[0],a[1]] for a in gpdmb.values.tolist()]
    cn.executemany('INSERT OR IGNORE INTO GPDM (GPDM,GPMC) VALUES (?,?)', data)

    cn.commit()
    cn.close()    
    

##########################################################################
#股票质押文件名
##########################################################################
def gpzyfn():
    config = iniconfig()
    ddir=os.path.join(getdrive(),readkey(config,'dldir'))
    today=datetime.datetime.now()
    n = datetime.datetime.weekday(today)
    t1=(today+datetime.timedelta(-8-n)).strftime("%Y%m%d")
    t2=(today+datetime.timedelta(-2-n)).strftime("%Y%m%d")
    fn="gpzyhgmx_" + t1 +"_" +t2 + ".xls"

    return os.path.join(ddir,fn)

##########################################################################
#获取中证指数网公布的最新市盈率、市净率和所属行业
##########################################################################
def get_pepb_zzhy():

    fn=syl_pe_fn('syldir')
    
    print('syl_pe_fn='+fn)
    
    df=get_pe_hy(fn)

    return df

    
##########################################################################
#获取最新市盈率文件名
##########################################################################
def syl_pe_fn(pedir):
    
    pedir = 'pedir' if pedir=='pedir' else 'syldir'
    pref = '' if pedir=='pedir' else 'csi'
    
    config = iniconfig()
    pedir=os.path.join(getdrive(),readkey(config,pedir))
    files = os.listdir(pedir)
    fs = [re.findall('.*(\d{8})\.xls',e) for e in files]
    jyrqlist =[]
    for e in fs:
        if len(e)>0:
            jyrqlist.append(e[0])

    jyr= sorted(jyrqlist,reverse=1)

    return os.path.join(pedir,pref+jyr[0]+'.xls')


########################################################################
#读取个股中证行业、静态市盈率、滚动市盈率、市净率
########################################################################
def get_pe_hy(file):
    wb = xlrd.open_workbook(file,encoding_override="cp1252")
    table = wb.sheet_by_name("个股数据")
    nrows = table.nrows #行数
    data =[]
    for rownum in range(1,nrows):
        row = table.row_values(rownum)
        dm=row[0]
        dm=dm+('.SH' if dm[0]=='6' else '.SZ')
        rowdat = [dm,row[9],row[10],row[11],row[12]]
        rowdat = [e if e!='-' else None for e in rowdat]
        data.append(rowdat)
    
    cols=['gpdm','zzhy','pe_lyr','pe_ttm','pb']
    df = pd.DataFrame(data,columns=cols)
    df = df.set_index('gpdm')
    df['pe_lyr']=df['pe_lyr'].astype(float)
    df['pe_ttm']=df['pe_ttm'].astype(float)
    df['pb']=df['pb'].astype(float)
    
    return df

##########################################################################
#生成字段名字典
##########################################################################
def flddic(nf):
    
    fld_dic={'股票代码':'gpdm','股票简称':'gpmc'}
    
    fld = [['基本每股收益(元)','eps0'],
            ['稀释每股收益(元)','eps1'],
            ['基本每股收益(同比增长率)(%)','eps0_g'],
            ['稀释每股收益(同比增长率)(%)','eps1_g'],
            ['营业总收入(同比增长率)(%)','yyzsr_g'],
            ['营业收入(同比增长率)(%)','yysr_g'],
            ['净利润同比增长率(%)','jlr_g'],
            ['扣除非经常性损益后的净利润同比增长率(%)','kfjlr_g'],
            ['营业收入(元)','yysr'],
            ['净利润(元)','jlr'],
            ['非经常性损益(元)','fsy'],
            ['上年同期净利润(元)','snjlr'],
            ['经营现金流量净额(元)','jyxjl'],
            ['资产总计(元)','zczj'],
            ['流动资产(元)','ldzc'],
            ['净资产(元)','jzc'],
            ['商誉(元)','sy'],
            ['带息债务(元)','dxzw'],
            ['应收账款(元)','yszk'],
            ['加权净资产收益率(%)','roe']
            ]
    
    for key,value in fld:
        key = key + nf + '.12.31'
        fld_dic[key] = value
        
    return fld_dic
    
##########################################################################
#读取eps数据
##########################################################################
def read_eps(xlsfn):
    wb = xw.Book(xlsfn)
        
    nf=re.findall('.+_(\d{4})\.xls',xlsfn)[0]
    
    
    #生成字段名字典
    fld_dic=flddic(nf)
    
    c = len(xw.Range('A1').expand('right').columns)
    #修改字段名
    for i in range(1,c+1):
        fldn = xw.Range((1,i)).value
        
        if fldn in fld_dic.keys() :
            xw.Range((1,i)).value = fld_dic[fldn]
    
    #读取数据
    data = wb.sheets[0].range('A1').options(pd.DataFrame, expand='table').value

    '''下面的语句很重要，MultiIndex转换成Index'''
    data.columns=[e[0] for e in data.columns]
    
    ''' 注意：数据列的元素数据类型有两种：str、float，运行下条语句后都变成了numpy.float64'''
    '''下面的语句很重要，运行后面的保留小数位数就不会出错'''
    data=data.replace('--',np.nan)   

    data=data.drop('gpmc',axis=1)

    '''保留2位小数必须在data=data.replace(np.nan,'--') 前执行
    注意：执行round(2)必须保证同一列各元素的数据类型是一致的,float和numpy.float64是两种不同的类型
    '''
    data=data.round(2)

    '''只保留至少有2项有效数字的行'''
    data=data.dropna(thresh=2)  
    
    data=data.replace(np.nan,'--') 

    data['rq']=nf+'.12.31'
    data['dm']=data.index

#    wb.close()
    xw.apps[0].quit()
    
    return data[['dm','rq','eps0','eps1','eps0_g','eps1_g']]
    
##########################################################################
#读取growth数据
##########################################################################
def read_growth(xlsfn):
    wb = xw.Book(xlsfn)
        
    nf=re.findall('.+_(\d{4})\.xls',xlsfn)[0]
    
    #生成字段名字典
    fld_dic=flddic(nf)
    
    c = len(xw.Range('A1').expand('right').columns)
    #修改字段名
    for i in range(1,c+1):
        fldn = xw.Range((1,i)).value
        
        if fldn in fld_dic.keys() :
            xw.Range((1,i)).value = fld_dic[fldn]
    
    #读取数据
    data = wb.sheets[0].range('A1').options(pd.DataFrame, expand='table').value

    '''下面的语句很重要，MultiIndex转换成Index'''
    data.columns=[e[0] for e in data.columns]

    #删除有效数据少于2项的股票
    data=data.replace('--',np.nan) 
    data=data.drop('gpmc',axis=1)
    data=data.dropna(thresh=2)  

    #单位换算
    data['yysr'] = data['yysr'].map(y2yy)
    data['jlr'] = data['jlr'].map(y2wy)
    data['fsy'] = data['fsy'].map(y2wy)
    data['snjlr'] = data['snjlr'].map(y2wy)


    '''保留2位小数必须在data=data.replace(np.nan,'--') 前执行'''
    data=data.round(2)
    data=data.replace(np.nan,'--') 

    data['rq'] = nf + '.12.31'
    data['dm'] = data.index

#    wb.close()
    xw.apps[0].quit()
    
    return data[['dm','rq','yyzsr_g','yysr_g','jlr_g','kfjlr_g','yysr','jlr','fsy','snjlr']]

##########################################################################
#读取rpt年报数据
##########################################################################
def read_rpt(xlsfn):
    wb = xw.Book(xlsfn)
        
    nf=re.findall('.+_(\d{4})\.xls',xlsfn)[0]
    
    #生成字段名字典
    fld_dic=flddic(nf)
    
    c = len(xw.Range('A1').expand('right').columns)
    #修改字段名
    for i in range(1,c+1):
        fldn = xw.Range((1,i)).value
        
        if fldn in fld_dic.keys() :
            xw.Range((1,i)).value = fld_dic[fldn]
    
    #读取数据
    data = wb.sheets[0].range('A1').options(pd.DataFrame, expand='table').value
    
    '''下面的语句很重要，MultiIndex转换成Index'''
    data.columns=[e[0] for e in data.columns]
    
    #删除有效数据少于2项的股票
    data=data.replace('--',np.nan) 
#    data=data.drop('gpmc',axis=1)
    data=data.dropna(thresh=2)  
    """
    股票年报关键数据：股票代码，日期，经营现金流量净额，营业收入，净利润，资产总计，流动资产，净资产，
    商誉，带息债务，应收账款，加权净资产收益率，杠杆倍数，带息负债率，商誉净资产占比，商誉总资产占比，
    收入经营现金含量，净利润经营现金含量，应收账款收入占比
    算法：
    杠杆倍数(权益乘数)=资产总计/净资产
    带息负债率=带息债务/资产总计
    商誉净资产占比=商誉/净资产
    商誉总资产占比=商誉/总资产
    收入经营现金含量=经营现金流量净额/营业收入
    净利润经营现金含量=经营现金流量净额/净利润
    应收账款收入占比=应收账款/营业收入
    """

    #单位换算JYXJL,YYSR,JLR,ZCZJ,LDZC,JZC,SY,DXZW,YSZK,ROE,GGBS,DXFZL,SYJZCZB,SYZZCZB,SRXJLHL,LRXJLHL,YSZKSRZB
    data['jyxjl'] = data['jyxjl'].map(y2wy)
    data['yysr'] = data['yysr'].map(y2wy)
    data['jlr'] = data['jlr'].map(y2wy)
    
    data['zczj'] = data['zczj'].map(y2yy)
    data['ldzc'] = data['ldzc'].map(y2yy)
    data['jzc'] = data['jzc'].map(y2yy)
    data['sy'] = data['sy'].map(y2yy)
    
    data['dxzw'] = data['dxzw'].map(y2yy)
    
    data['yszk'] = data['yszk'].map(y2wy)


    data.eval('ggbs = zczj / jzc',inplace=True)

    data.eval('dxfzl = dxzw / zczj * 100',inplace=True)
    
    data.eval('syjzczb = sy / jzc * 100',inplace=True)

    data.eval('syzzczb = sy / zczj * 100',inplace=True)

    data.eval('srxjlhl = jyxjl / yysr * 100',inplace=True)

    data.eval('lrxjlhl = jyxjl / jlr * 100',inplace=True)

    data.eval('yszksrzb = yszk / yysr * 100',inplace=True)
    
    data['rq'] = nf + '.12.31'
    data['dm'] = data.index

    '''保留2位小数必须在data=data.replace(np.nan,'--') 前执行'''
    data=data.round(2)
    data=data.replace(np.nan,'--') 

    xw.apps[0].quit()

    return data[['dm','rq','jyxjl','yysr','jlr','zczj','ldzc','jzc','sy','dxzw',
               'yszk','roe','ggbs','dxfzl','syjzczb','syzzczb','srxjlhl','lrxjlhl','yszksrzb']]

##########################################################################
#写入eps数据
##########################################################################
def write_eps():
    
    createDataBase()
    
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    cn = sqlite3.connect(dbfn)

    config = iniconfig()
    sn = int(readkey(config,'sn'))
    qn = int(readkey(config,'qn'))
    
    ddir=os.path.join(getdrive(),readkey(config,'dldir'))

    for j in range(sn,qn+1):
#        newfn = readkey(config,'EPS_'+str(j)+'.xls')
        newfn = 'EPS_'+str(j)+'.xls'        
        xlsfn = os.path.join(ddir,newfn)

        epsdf=read_eps(xlsfn)

        data=epsdf.values.tolist()
        
        cn.executemany('INSERT OR IGNORE INTO GPEPS (GPDM,RQ,EPS0,EPS1,EPS0_G,EPS1_G) VALUES (?,?,?,?,?,?)', data)

        cn.commit()
        
    cn.close()    


##########################################################################
#写入财报数据
##########################################################################
def write_nb():
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    cn = sqlite3.connect(dbfn)

    config = iniconfig()
    sn = int(readkey(config,'sn'))
    qn = int(readkey(config,'qn'))
    
    ddir=os.path.join(getdrive(),readkey(config,'dldir'))

    for j in range(sn,qn+1):
        newfn = readkey(config,'rptfn'+str(j))
        xlsfn = os.path.join(ddir,newfn)

        df=read_rpt(xlsfn)

        data=df.values.tolist()
        
        cn.executemany('''INSERT OR REPLACE INTO GPNB 
                       (GPDM,RQ,JYXJL,YYSR,JLR,ZCZJ,LDZC,JZC,SY,DXZW,YSZK,ROE,
                       GGBS,DXFZL,SYJZCZB,SYZZCZB,SRXJLHL,LRXJLHL,YSZKSRZB)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', data)

        cn.commit()
        
    cn.close()    

##########################################################################
#元转成亿元
##########################################################################
def y2yy(num):
    try :
        return num/100000000
    except:
        return num

##########################################################################
#元转成万元
##########################################################################
def y2wy(num):
    try :
        return num/10000
    except:
        return num
    
##########################################################################
#写入growth数据
##########################################################################
def write_growth():
    
    createDataBase()
    prefix = 'czx'
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    cn = sqlite3.connect(dbfn)

    config = iniconfig()
    nf1 = int(readkey(config,prefix + 'nf1'))
    nf2 = int(readkey(config,prefix + 'nf2'))

    newfn0 = readkey(config, prefix + 'fn')
    
    ddir=os.path.join(getdrive(),readkey(config,'dldir'))

    for j in range(nf1,nf2+1):

        newfn = newfn0+str(j)+'.xls'        
        xlsfn = os.path.join(ddir,newfn)

        df=read_growth(xlsfn)

        data=df.values.tolist()
        
        cn.executemany('INSERT OR IGNORE INTO GPGROWTH (GPDM,RQ,YYZSR_G,YYSR_G,JLR_G,KFJLR_G,YYSR,JLR,FSY,SNJLR) VALUES (?,?,?,?,?,?,?,?,?,?)', data)

        cn.commit()
        
    cn.close()    

'''
python pandas 组内排序、单组排序、标号     
http://blog.csdn.net/qq_22238533/article/details/72395564    

pandas如何去掉、过滤数据集中的某些值或者某些行？ 
http://blog.csdn.net/qq_22238533/article/details/76127966

基于财务因子的多因子选股模型
https://www.windquant.com/qntcloud/v?3540281b-9a75-4506-adb6-983cf5091e74

选股条件：
2015年营业总收入同比增长率>20% 2016年营业总收入同比增长率>20% 
2016年销售毛利率>2015年销售毛利率 
2016年销售净利率>2015年销售净利率 
2017年业绩预增 上市时间在2016年1月以前 
2016年roe>10 2015年roe>10
'''

##########################################################################
#上市日期变成yyyy-mm-dd
##########################################################################
def ssrqstr(num):
    s=str(num)
    return s[:4]+'-'+s[4:6]+'-'+s[6:8]
    
##########################################################################
#清空指定表,没有则创建
##########################################################################
def clearsheet(shtn):
    shtexist = False
    for sht in xw.sheets:
        if sht.name in shtn :
            sht.activate()
            sht.clear_contents()
            shtexist = True
            return sht
            
    if not shtexist:
        return xw.sheets.add(name=shtn) 

        
##########################################################################
#查询2013-2016年yysr
##########################################################################
def query_yysr(nf):
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    nbrq=str(nf)+'.12.31'
    #营业收入yysr、净利润jlr单位为：万元
    sql = 'select gpdm,yysr from gpnb where rq="'+nbrq+'";'
    
    curs.execute(sql)
    
    data = curs.fetchall()
    cols = ['gpdm','yysr'+str(nf)]
    
    df = pd.DataFrame(data,columns=cols)
    df = df.set_index('gpdm')

    #营业收入yysr、净利润jlr单位转换为：亿元    
    df['yysr'+str(nf)]=df['yysr'+str(nf)].map(lambda x:x/10000)

    dbcn.close()    

    return df    
    
##########################################################################
#查询2013-2016年jlr
##########################################################################
def query_jlr(nf):
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    nbrq=str(nf)+'.12.31'
    #营业收入yysr、净利润jlr单位为：万元
    sql = 'select gpdm,jlr from gpnb where rq="'+nbrq+'";'
    
    curs.execute(sql)
    
    data = curs.fetchall()
    cols = ['gpdm','jlr'+str(nf)]
    
    df = pd.DataFrame(data,columns=cols)
    df = df.set_index('gpdm')

    #营业收入yysr、净利润jlr单位转换为：亿元    
    df['jlr'+str(nf)]=df['jlr'+str(nf)].map(lambda x:x/10000)

    dbcn.close()    

    return df    
##########################################################################
#查询2013-2016年roe
##########################################################################
def query_roe(nf):
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    nbrq=str(nf)+'.12.31'
    sql = 'select gpdm,roe from gpnb where rq="'+nbrq+'";'
    
    curs.execute(sql)
    
    data = curs.fetchall()
    cols = ['gpdm','roe'+str(nf)]
    
    df = pd.DataFrame(data,columns=cols)
    df = df.set_index('gpdm')

    dbcn.close()    

    return df    

##########################################################################
#查询2013-2016年roa
##########################################################################
def query_roa(nf):
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    nbrq=str(nf)+'.12.31'
    sql = 'select gpdm,roa from gproa where rq="'+nbrq+'";'
    
    curs.execute(sql)
    
    data = curs.fetchall()
    cols = ['gpdm','roa'+str(nf)]
    
    df = pd.DataFrame(data,columns=cols)
    df = df.set_index('gpdm')

    dbcn.close()    

    return df    

##########################################################################
#查询2013-2016年jll净利率
##########################################################################
def query_jll(nf):
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    nbrq=str(nf)+'.12.31'
    sql = 'select gpdm,jll from gproa where rq="'+nbrq+'";'
    
    curs.execute(sql)
    
    data = curs.fetchall()
    cols = ['gpdm','jll'+str(nf)]
    
    df = pd.DataFrame(data,columns=cols)
    df = df.set_index('gpdm')

    dbcn.close()    

    return df    

##########################################################################
#查询2013-2016年jll净利率
##########################################################################
def query_mll(nf):
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    nbrq=str(nf)+'.12.31'
    sql = 'select gpdm,mll from gproa where rq="'+nbrq+'";'
    
    curs.execute(sql)
    
    data = curs.fetchall()
    cols = ['gpdm','mll'+str(nf)]
    
    df = pd.DataFrame(data,columns=cols)
    df = df.set_index('gpdm')

    dbcn.close()    

    return df    


##########################################################################
#查询2013-2016年收入自由现金含量
##########################################################################
def query_srzyxjhl1(nf):
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    nbrq=str(nf)+'.12.31'
    sql = 'select gpdm,srzyxjhl1 from gproa where rq="'+nbrq+'";'
    
    curs.execute(sql)
    
    data = curs.fetchall()
    cols = ['gpdm','srzyxjhl1_'+str(nf)]
    
    df = pd.DataFrame(data,columns=cols)
    df = df.set_index('gpdm')

    dbcn.close()    

    return df    

##########################################################################
#查询2013-2016年收入自由现金含量
##########################################################################
def query_srzyxjhl2(nf):
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    nbrq=str(nf)+'.12.31'
    sql = 'select gpdm,srzyxjhl2 from gproa where rq="'+nbrq+'";'
    
    curs.execute(sql)
    
    data = curs.fetchall()
    cols = ['gpdm','srzyxjhl2_'+str(nf)]
    
    df = pd.DataFrame(data,columns=cols)
    df = df.set_index('gpdm')

    dbcn.close()    

    return df    

##########################################################################
#查询nf年报eps0基本每股收益
##########################################################################
def query_eps(nf):
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    nbrq=str(nf)+'.12.31'
    sql = 'select gpdm,eps0 from gpeps where rq="'+nbrq+'";'
    
    curs.execute(sql)
    
    data = curs.fetchall()
    cols = ['gpdm','eps'+str(nf)]
    
    df = pd.DataFrame(data,columns=cols)
    df = df.set_index('gpdm')

    dbcn.close()    

    return df    
            
##########################################################################
#查询nf年报eps0基本每股收益
##########################################################################
def query_epsg(nf):
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    nbrq=str(nf)+'.12.31'
    sql = 'select gpdm,eps0_g from gpeps where rq="'+nbrq+'";'
    
    curs.execute(sql)
    
    data = curs.fetchall()
    cols = ['gpdm','eps_g'+str(nf)]
    
    df = pd.DataFrame(data,columns=cols)
    df = df.set_index('gpdm')

    dbcn.close()    

    return df    
            
            
##########################################################################
#筛选2015和2016年营业收入和净利润都基本保持增长或减幅在5%以内的生成股票池
##########################################################################
def stkpool_grth():    
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    
    curs.execute('''select a.gpdm from 
                 (select gpdm from gpgrowth where rq=='2015.12.31' and yyzsr_g>-5 and yysr_g>-5 and jlr>0 and jlr_g>-5) as a, 
                 (select gpdm from gpgrowth where rq=='2016.12.31' and yyzsr_g>-5 and yysr_g>-5 and jlr>0 and jlr_g>-5) as b 
                 where a.gpdm=b.gpdm
                 ;''')
    
    data = curs.fetchall()
    data = [e[0] for e in data]
    dbcn.close()    

    return data

##########################################################################
#筛选2015和2016年roe>10的生成股票池
##########################################################################
def stkpool_roe():    
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    
    curs.execute('''select a.gpdm from 
                 (select gpdm from gpnb where rq=='2015.12.31' and roe>0) as a, 
                 (select gpdm from gpnb where rq=='2016.12.31' and roe>5) as b 
                 where a.gpdm=b.gpdm
                 ;''')
    
    data = curs.fetchall()
    data = [e[0] for e in data]
    dbcn.close()    

    return data
            
##########################################################################
#筛选2015和2016年基本eps增长率>0,eps>0
##########################################################################
def stkpool_eps0_g():    
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    
    curs.execute('''select a.gpdm from 
                    (select gpdm from gpeps where rq='2015.12.31' and eps0_g>-5 and eps0>0) as a,
                    (select gpdm from gpeps where rq='2016.12.31' and eps0_g>-5 and eps0>0) as b
                    where a.gpdm=b.gpdm
                 ;''')
    
    data = curs.fetchall()
    data = [e[0] for e in data]
    dbcn.close()    

    return data

##########################################################################
#筛选2015和2016年基本eps增长率>0,eps>0
##########################################################################
def stkpool_2016():    
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    #2016年基本PES>0.05，基本EPS增长率>-5%，营业总收入增长率>-5%，营业收入增长率>-5%，净利润增长率>-5%，净利润>0，净资产收益率>10%
    curs.execute('''
                 select a.gpdm from
                 (select gpdm from gpeps where rq='2016.12.31' and eps0_g>-5 and eps0>0.05) as a,
                 (select gpdm from gpgrowth where rq=='2016.12.31' and yyzsr_g>-15 and yysr_g>-15 and jlr>0 and jlr_g>-5) as b,
                 (select gpdm from gpnb where rq=='2016.12.31' and roe>0) as c
                 where a.gpdm=b.gpdm and a.gpdm=c.gpdm and b.gpdm=c.gpdm
                 ;''')
    
    data = curs.fetchall()
    data = [e[0] for e in data]
    dbcn.close()    

    return data
            
##########################################################################
#筛选2016年商誉净资产占比>40的生成股票池
##########################################################################
def stkpool_syjzczb():    
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    
    curs.execute('''select gpdm from gpnb where rq='2016.12.31' and syjzczb>40
                 ;''')
    
    data = curs.fetchall()
    data = [e[0] for e in data]
    
    dbcn.close()    

    return data
            

##########################################################################
#选股
##########################################################################   
def selestk(wb):

    pool_1 = stkpool_grth()
    pool_2 = stkpool_roe()
    pool_3 = stkpool_syjzczb()
    pool_4 = stkpool_eps0_g()

    for bk in xw.books:
        if bk.name == '选股分析.xlsm' :
            opened = True
    if not opened :
        wb = xw.books.open(r'D:\selestock\选股分析.xlsm')

    wb.activate()    

    sht = wb.sheets('fld_dic')
    df = sht.range('A1').options(pd.DataFrame, expand='table').value
    
    #将列转换成单层索引
    df.columns=[e[0] for e in df.columns]
    
    #选取的字段
    cols=[row['fld_e'] for index, row in df.iterrows() if row['fld_s']==1]
    cols1=[row['fld_e'] for index, row in df.iterrows() if row['fld_1']==1]
    cols2=[row['fld_e'] for index, row in df.iterrows() if row['fld_2']==1]

    #重新设置索引
#    df['fld_c']=df.index
#    df['fld_n']=pd.to_numeric(df['fld_n'], downcast='unsigned')
#    df=df.set_index('fld_n',drop=False)
    fld_dic=df.fld_e
    
    sht = wb.sheets('selestock')
    
    sht.activate()
    
    
    c = len(xw.Range('A1').expand('right').columns)
    #修改字段名
    for i in range(1,c+1):
        fldn = xw.Range((1,i)).value
        
        if fldn in fld_dic.keys() :
            xw.Range((1,i)).value = fld_dic[fldn]

    df = sht.range('A1').options(pd.DataFrame, expand='table').value
    df.columns=[e[0] for e in df.columns]

    cols=[e for e in cols if e!=df.index.name]

    #转换NaN
    df=df.replace('--',np.nan) 
    
    #去掉无用的列
    df = df[cols] 

    '''筛选'''
    
    #转换上市日期，删去3年内上市的股票
    df['ssrq']=df['ssrq'].map(ssrqstr)
    df=df[(df['ssrq']<'2016-01-01')]

    #筛选roe和成长性   
    df =df[(df.index.isin(pool_1))]
    df =df[(df.index.isin(pool_2))]

    #删除商誉占比高德 ,注意pandas的取反运算符~   
    df = df[(~df.index.isin(pool_3))]

    #eps增长率
    df =df[(df.index.isin(pool_4))]

    #2018年2019年预测增长率
    df =df[(df['ycjlr2018_g']>0)]
    df =df[(df['ycjlr2019_g']>0)]
    
    #去掉上年亏损的和0.05以下的，因为eps在0.05以下的作假的概率大
    df=df[(df['eps0_1']>0.05)]
    

    '''下面是按预告的业绩选股'''
    #去掉上年净利润亏损的
    df1=df[(df['jlr_1']>0) & (df['ygjlr']>0) & (df['ygjlr_g']>0)]
    
    #计算预告eps
    df1 = df1.assign(ygeps0 = df1['ygjlr']/df1['zgb'])

    #计算pe
    df1 = df1.assign(ygpe = df1['gj']/df1['ygeps0'])

    #去掉pe大于100
    df1=df1[(df1['ygpe']<100)]
    
    #计算预告peg    
    df1 = df1.assign(ygpeg = df1['ygpe']/df1['ygjlr_g'])
    
    #保留peg<1
    df1=df1[(df1['ygpeg']>0) & (df1['ygpeg']<1)]
    
    # 排序peg
    df1 = df1.sort_values(by='ygpeg')

    df1=df1.round(4)
 
    cols1=[e for e in cols1 if e!=df1.index.name]
    df1=df1[cols1]
    
    sht = clearsheet('result1')
    
    sht.range('A1').value = df1

    '''下面是按公布的业绩选股'''
    #选出每股收益为正，并且同比增长率为正的
    df2=df[(df.eps0>0) & (df.eps0_g>0)]
    #计算pe和peg
    df2 = df2.assign(pe = df2['gj'] / df2['eps0'])
    df2 = df2.assign(peg = df2['pe'] / df2['eps0_g'])
    
    #保留peg<1
    df2=df2[(df2['peg']>0) & (df2['peg']<1)]
    
    # 排序peg
    df2 = df2.sort_values(by='peg')
    
    df2=df2.round(4)

    cols2=[e for e in cols2 if e!=df2.index.name]
    df2=df2[cols2]
    
    sht = clearsheet('result2')
      
    sht.range('A1').value = df2


##########################################################################
#从i问财网下载书
##########################################################################
def dl_iwencaixls():

    config = iniconfig()
    
    ddir=os.path.join(getdrive(),readkey(config,'dldir'))
    
    dafn = dlfn(ddir)
    
    newfn = os.path.join(ddir,readkey(config, 'fn'))
    if os.path.exists(newfn):
        os.remove(newfn)

    kw = readkey(config,'kw')
    sele = readkey(config,'sl')
    

    username = readkey(config,'iwencaiusername')
    pwd = readkey(config,'iwencaipwd')
    
    profile = webdriver.FirefoxProfile()
    profile.set_preference('browser.download.dir', ddir)
    profile.set_preference('browser.download.folderList', 2)
    profile.set_preference('browser.download.manager.showWhenStarting', False)
    
    #http://www.w3school.com.cn/media/media_mimeref.asp
    profile.set_preference('browser.helperApps.neverAsk.saveToDisk', 'application/vnd.ms-excel')
    
    browser = webdriver.Firefox(firefox_profile=profile)

    #浏览器窗口最大化
    browser.maximize_window()
    #登录同花顺
    browser.get("http://upass.10jqka.com.cn/login")
    #time.sleep(1)
    elem = browser.find_element_by_id("username")
    elem.clear()
    elem.send_keys(username)
    
    elem = browser.find_element_by_class_name("pwd")
    elem.clear()
    elem.send_keys(pwd)
    
    browser.find_element_by_id("loginBtn").click()
    time.sleep(2)
    

    browser.get("http://www.iwencai.com/")
    time.sleep(5)
    browser.find_element_by_id("auto").clear()
    browser.find_element_by_id("auto").send_keys(kw)
    browser.find_element_by_id("qs-enter").click()
    time.sleep(10)
    
    #打开查询项目选单
    trigger = browser.find_element_by_class_name("showListTrigger")
    trigger.click()
    time.sleep(1)
    
    #获取查询项目选单
    checkboxes = browser.find_elements_by_class_name("showListCheckbox")
    indexstrs = browser.find_elements_by_class_name("index_str")
    
    #去掉选项前的“√”
    #涨幅、股价保留
    for i in range(0,len(checkboxes)):
        checkbox=checkboxes[i]
        
        #对于“pe,ttm”之类中间的逗号用下划线代替，注意配置文件也需要这样
        indexstr=indexstrs[i].text.replace(",","_")
        
        if checkbox.is_selected() and not indexstr in sele :
            checkbox.click()
        if not checkbox.is_selected() and indexstr in sele :
            checkbox.click()

    #向上滚屏
    js="var q=document.documentElement.scrollTop=0"  
    browser.execute_script(js)  
    time.sleep(3) 
    
    #关闭查询项目选单
    trigger = browser.find_element_by_class_name("showListTrigger")
    trigger.click()
    time.sleep(3)
    
    #导出数据
    elem = browser.find_element_by_class_name("export.actionBtn.do") 
    #在html中类名包含空格
    elem.click() 
    time.sleep(10)

    if os.path.exists(dafn):
        os.rename(dafn,newfn)
        
    browser.quit()

    if os.path.exists(newfn):
        return newfn

    return 

##########################################################################
#从i问财网下载书
##########################################################################
def dl_iwencaixls2():

    config = iniconfig()
    
    ddir=os.path.join(getdrive(),readkey(config,'dldir'))
    
    dafn = dlfn(ddir)
    
    sn = int(readkey(config,'sn'))
    qn = int(readkey(config,'qn'))

    username = readkey(config,'iwencaiusername')
    pwd = readkey(config,'iwencaipwd')
    
    profile = webdriver.FirefoxProfile()
    profile.set_preference('browser.download.dir', ddir)
    profile.set_preference('browser.download.folderList', 2)
    profile.set_preference('browser.download.manager.showWhenStarting', False)
    
    #http://www.w3school.com.cn/media/media_mimeref.asp
    profile.set_preference('browser.helperApps.neverAsk.saveToDisk', 'application/vnd.ms-excel')
    
    browser = webdriver.Firefox(firefox_profile=profile)

    #浏览器窗口最大化
    browser.maximize_window()
    #登录同花顺
    browser.get("http://upass.10jqka.com.cn/login")
    #time.sleep(1)
    elem = browser.find_element_by_id("username")
    elem.clear()
    elem.send_keys(username)
    
    elem = browser.find_element_by_class_name("pwd")
    elem.clear()
    elem.send_keys(pwd)
    
    browser.find_element_by_id("loginBtn").click()
    time.sleep(2)
    
    for j in range(sn,qn+1):
        
        kw = readkey(config,'kw'+str(j))
        sele = readkey(config,'sl'+str(j))
        newfn = os.path.join(ddir,readkey(config, 'fn'+str(j)))
        
        if os.path.exists(newfn):
            os.remove(newfn)
        
        browser.get("http://www.iwencai.com/")
        time.sleep(5)
        browser.find_element_by_id("auto").clear()
        browser.find_element_by_id("auto").send_keys(kw)
        browser.find_element_by_id("qs-enter").click()
        time.sleep(10)
        
        #打开查询项目选单
        trigger = browser.find_element_by_class_name("showListTrigger")
        trigger.click()
        time.sleep(1)
        
        #获取查询项目选单
        checkboxes = browser.find_elements_by_class_name("showListCheckbox")
        indexstrs = browser.find_elements_by_class_name("index_str")
        
        #去掉选项前的“√”
        #涨幅、股价保留
        for i in range(0,len(checkboxes)):
            checkbox=checkboxes[i]
            
            #对于“pe,ttm”之类中间的逗号用下划线代替，注意配置文件也需要这样
            indexstr=indexstrs[i].text.replace(",","_")
            
            if checkbox.is_selected() and not indexstr in sele :
                checkbox.click()
            if not checkbox.is_selected() and indexstr in sele :
                checkbox.click()
    
        #向上滚屏
        js="var q=document.documentElement.scrollTop=0"  
        browser.execute_script(js)  
        time.sleep(3) 
        
        #关闭查询项目选单
        trigger = browser.find_element_by_class_name("showListTrigger")
        trigger.click()
        time.sleep(3)
        
        #导出数据
        elem = browser.find_element_by_class_name("export.actionBtn.do") 
        #在html中类名包含空格
        elem.click() 
        time.sleep(10)
    
        if os.path.exists(dafn):
            os.rename(dafn,newfn)
            
    browser.quit()

    return 

##########################################################################
#查询roe
##########################################################################
def get_roe(nf1,nf2,v=None):
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    for nf in range(nf1,nf2):
        nbrq=str(nf)+'.12.31'
        sql = 'select gpdm,roe from gpnb where rq="'+nbrq+'";'
        
        curs.execute(sql)
        
        data = curs.fetchall()
        cols = ['gpdm','roe'+str(nf)]
        
        df1 = pd.DataFrame(data,columns=cols)
        df1 = df1.set_index('gpdm')
        if nf==nf1 :
            df=df1
        else:
            df=df.join(df1)

    dbcn.close()    

    if v != None :
        for nf in range(nf1,nf2):
            df = df[(df['roe'+str(nf)]>=v)]


    return df

##########################################################################
#查询roa
##########################################################################
def get_roa(nf1,nf2,v=None):
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    for nf in range(nf1,nf2):
        nbrq=str(nf)+'.12.31'
        sql = 'select gpdm,roa from gproa where rq="'+nbrq+'";'
        
        curs.execute(sql)
        
        data = curs.fetchall()
        cols = ['gpdm','roa'+str(nf)]
        
        df1 = pd.DataFrame(data,columns=cols)
        df1 = df1.set_index('gpdm')
        if nf==nf1 :
            df=df1
        else:
            df=df.join(df1)

    dbcn.close()    

    if v != None :
        for nf in range(nf1,nf2):
            df = df[(df['roa'+str(nf)]>=v)]


    return df

##########################################################################
#查询净利率
##########################################################################
def get_jll(nf1,nf2,v=None):
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    for nf in range(nf1,nf2):
        nbrq=str(nf)+'.12.31'
        sql = 'select gpdm,jll from gproa where rq="'+nbrq+'";'
        
        curs.execute(sql)
        
        data = curs.fetchall()
        cols = ['gpdm','jll'+str(nf)]
        
        df1 = pd.DataFrame(data,columns=cols)
        df1 = df1.set_index('gpdm')
        if nf==nf1 :
            df=df1
        else:
            df=df.join(df1)

    dbcn.close()    

    if v != None :
        for nf in range(nf1,nf2):
            df = df[(df['jll'+str(nf)]>=v)]


    return df

##########################################################################
#查询营业收入自由现金流含量
##########################################################################
def get_srzyxjhl2(nf1,nf2,v=None):
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    for nf in range(nf1,nf2):
        nbrq=str(nf)+'.12.31'
        sql = 'select gpdm,srzyxjhl2 from gproa where rq="'+nbrq+'";'
        
        curs.execute(sql)
        
        data = curs.fetchall()
        cols = ['gpdm','srzyxjhl2_'+str(nf)]
        
        df1 = pd.DataFrame(data,columns=cols)
        df1 = df1.set_index('gpdm')
        if nf==nf1 :
            df=df1
        else:
            df=df.join(df1)

    dbcn.close()    
    
    if v != None :
        for nf in range(nf1,nf2):
            df = df[(df['srzyxjhl2_'+str(nf)]>=v)]

    return df

##########################################################################
#查询营业收入自由现金流含量
##########################################################################
def get_srzyxjhl1(nf1,nf2,v=None):
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    for nf in range(nf1,nf2):
        nbrq=str(nf)+'.12.31'
        sql = 'select gpdm,srzyxjhl1 from gproa where rq="'+nbrq+'";'
        
        curs.execute(sql)
        
        data = curs.fetchall()
        cols = ['gpdm','srzyxjhl1_'+str(nf)]
        
        df1 = pd.DataFrame(data,columns=cols)
        df1 = df1.set_index('gpdm')
        if nf==nf1 :
            df=df1
        else:
            df=df.join(df1)

    dbcn.close()    
    
    if v != None :
        for nf in range(nf1,nf2):
            df = df[(df['srzyxjhl1_'+str(nf)]>=v)]

    return df

##########################################################################
#查询利润现金流含量
##########################################################################
def get_lrxjlhl(nf1,nf2):
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    for nf in range(nf1,nf2):
        nbrq=str(nf)+'.12.31'
        sql = 'select gpdm,lrxjlhl from gpnb where rq="'+nbrq+'";'
        
        curs.execute(sql)
        
        data = curs.fetchall()
        cols = ['gpdm','lrxjlhl'+str(nf)]
        
        df1 = pd.DataFrame(data,columns=cols)
        df1 = df1.set_index('gpdm')
        if nf==nf1 :
            df=df1
        else:
            df=df.join(df1)

    dbcn.close()    

    return df


##########################################################################
#股票基本信息
#参考\tushare\stock\fundamental.py
#def get_stock_basics(date=None)
#               code,代码
#               name,名称
#               industry,细分行业
#               area,地区
#               pe,市盈率
#               outstanding,流通股本
#               totals,总股本(万)
#               totalAssets,总资产(万)
#               liquidAssets,流动资产
#               fixedAssets,固定资产
#               reserved,公积金
#               reservedPerShare,每股公积金
#               eps,每股收益
#               bvps,每股净资
#               pb,市净率
#               timeToMarket,上市日期  
#               undp,未分配利润(万元)
#               perundp,每股未分配利润  
#               rev,最新财报营业总收入增长率(%)  不能确定含义？？？？？？
#               profit,最新财报净利润增长率(%) 净利润同比(%)   不能确定含义？？？？？？
#               gpr,销售毛利率(%)
#               npr,净利润率   
#               holders,股东户数    
#tushare保存目录：
#http://file.tushare.org/tsdata/201904/all20190426.csv
#    request = Request(ct.ALL_STOCK_BASICS_FILE%(datepre, '' if date is None else wdate))
#http://file.tushare.org/tsdata/all.csv为最近一个交易日的数据   
#该文件好像收盘后
##########################################################################
def get_stock_basics():

    csvfn = r'D:\selestock\all.csv'

    #如果文件是最后一个交易日23:00以后生成的，则不需要下载
    dlyes=dl_fn(csvfn,23)

    if dlyes:
        urlfn='http://file.tushare.org/tsdata/all.csv'
        req = request.Request(urlfn)
        text = request.urlopen(req, timeout=10).read()
        text = text.decode('GBK')
        text = text.replace('--', '')
        df = pd.read_csv(StringIO(text), dtype={'code':'object'})
        df = df.set_index('code')
#        df=ts.get_stock_basics()
        df.to_csv(csvfn,encoding='GBK')
    else:
        df = pd.read_csv(csvfn, dtype={'code':'object'}, encoding='GBK')
        df = df.set_index('code')
    return df


##########################################################################
#查询业绩预告
##########################################################################
def get_yjyg(nf):
    
    df = get_stock_basics()
    gpgb = df['totals']*100000000       #股
    gpgb.index=[e+('.SH' if e[0]=='6' else '.SZ') for e in gpgb.index]
    gpgb.index.name='gpdm'

    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    nbrq=str(nf)+'.12.31'
    sql = '''select gpdm,yjbdfd,jlr_1,(1+yjbdfd/100)*jlr_1 as jlr from yjyg where jlr_1>0 and rq=?
            union select gpdm,yjbdfd,jlr_1,(1-yjbdfd/100)*jlr_1 as jlr from yjyg where jlr_1<0 and rq=?
            ;'''
        
    curs.execute(sql,(nbrq,nbrq))
    
    data = curs.fetchall()
    cols = ['gpdm','jlr_g','jlr_1','jlr']
    
    df = pd.DataFrame(data,columns=cols)
    dbcn.close()    

    df = df.set_index('gpdm')
    df=df.join(gpgb)
    df['yysr']=np.nan
    df['yysr_1']=np.nan
    df['yysr_g']=np.nan
    df['mgjzc']=np.nan
    df['roe']=np.nan
    df['yjbg']='预告'
    
    df.eval('eps = jlr / totals',inplace=True)
    cols = ['eps','yysr','yysr_1','yysr_g','jlr','jlr_1','jlr_g','mgjzc','roe','yjbg']
    
    return df[cols]


##########################################################################
#查询业绩预告
##########################################################################
def yjyg_g(x):
    if x==None:
        g = None
    elif not '～' in x:
        g = eval(x.replace('%',''))
    elif '～' in x:
        g = eval(x.replace('～','+').replace('%',''))/2 
    else:
        g = None
      
    if g==None:
        yj = ""
    elif g>0:
        yj = "预告：净利润同比增加%d%%" % g
    elif g<0:
        yj = "预告：净利润同比减少%d%%" % g
    else:
        yj = ""
        
    return (yj,g)

##########################################################################
#查询业绩预告
##########################################################################
def get_yjyg_bgq(nbrq):
        
    nbrq=dateutil.parser.parse(nbrq).strftime("%Y.%m.%d")

    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()

    sql = '''select gpdm,yjbdfw,yglx,jlr from yjyg where rq=?;'''
        
    curs.execute(sql,(nbrq,))
    
    data = curs.fetchall()
    cols = ['gpdm','yjbdfw','yglx','jlr']
    
    df = pd.DataFrame(data,columns=cols)
    dbcn.close()    

    df = df.set_index('gpdm')
    yjdf=df['yjbdfw'].map(yjyg_g)

    yjdf=yjdf.to_frame()

    yjdf['yj']=[e[0] for e in yjdf['yjbdfw']]
    yjdf['jlr_g_bgq']=[e[1] for e in yjdf['yjbdfw']]

    yjdf=yjdf.drop(columns=['yjbdfw'])

    df = df.join(yjdf)

    df['jlr']=df['jlr']/10000   #万元
        
    return df[['yj','jlr_g_bgq','jlr']]

##########################################################################
#查询业绩快报
##########################################################################
def get_yjkb_bgq(nbrq):
        
    nbrq=dateutil.parser.parse(nbrq).strftime("%Y.%m.%d")

    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()

    sql = '''select gpdm,jlr_g,jlr from yjkb where not (jlr_g isnull) and rq=?;'''
        
    curs.execute(sql,(nbrq,))
    
    data = curs.fetchall()
    cols = ['gpdm','jlr_g_bgq','jlr']
    
    df = pd.DataFrame(data,columns=cols)
    dbcn.close()    

    df = df.set_index('gpdm')
    df = df.assign(yj=df['jlr_g_bgq'].map(lambda x:'快报：'+('净利润同比增加'+str(x) if x>0 else '净利润同比减少'+str(x)))+'%')
    df['jlr']=df['jlr']/10000   #万元
    return df[['yj','jlr_g_bgq','jlr']]

##########################################################################
#查询业绩报表
##########################################################################
def get_yjbb_bgq(nbrq):
        
    nbrq=dateutil.parser.parse(nbrq).strftime("%Y.%m.%d")

    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()

    sql = '''select gpdm,jlr_g,jlr from yjbb where not (jlr_g isnull) and rq=?;'''
        
    curs.execute(sql,(nbrq,))
    
    data = curs.fetchall()
    cols = ['gpdm','jlr_g_bgq','jlr']
    
    df = pd.DataFrame(data,columns=cols)
    dbcn.close()    

    df = df.set_index('gpdm')
    df = df.assign(yj=df['jlr_g_bgq'].map(lambda x:'报表：'+('净利润同比增加'+str(x) if x>0 else '净利润同比减少'+str(x)))+'%')
    df['jlr']=df['jlr']/10000   #万元
    return df[['yj','jlr_g_bgq','jlr']]


##########################################################################
#查询最新业绩情况
##########################################################################
def get_zxyj_bgq(nbrq):
    yjbb = get_yjbb_bgq(nbrq)       
    yjkb = get_yjkb_bgq(nbrq)       
    yjyg = get_yjyg_bgq(nbrq)  

    yjkb = yjkb[~(yjkb.index.isin(yjbb.index))]   
    yjyg = yjyg[~(yjyg.index.isin(yjbb.index))]
    yjyg = yjyg[~(yjyg.index.isin(yjkb.index))]
    
    zxyj=pd.concat([yjbb,yjkb,yjyg])
    
    return zxyj


##########################################################################
#查询业绩快报
##########################################################################
def get_yjkb(nf):
    
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    nbrq=str(nf)+'.12.31'
    sql = 'select gpdm,eps,yysr,yysr_1,yysr_g,jlr,jlr_1,jlr_g,mgjzc,roe from yjkb where rq="'+nbrq+'";'
    
    curs.execute(sql)
    
    data = curs.fetchall()
    cols = ['gpdm','eps','yysr','yysr_1','yysr_g','jlr','jlr_1','jlr_g','mgjzc','roe']
    
    df = pd.DataFrame(data,columns=cols)
    df = df.set_index('gpdm')

    dbcn.close()    

    df['yjbg']='快报'
    cols = ['eps','yysr','yysr_1','yysr_g','jlr','jlr_1','jlr_g','mgjzc','roe','yjbg']
    
    return df[cols]

##########################################################################
#查询业绩报表
##########################################################################
def get_yjbb(nf):
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    nbrq=str(nf)+'.12.31'
    sql = 'select gpdm,eps,yysr,yysr_g,jlr,jlr_g,mgjzc,roe from yjbb where rq="'+nbrq+'";'
    
    curs.execute(sql)
    
    data = curs.fetchall()
    cols = ['gpdm','eps','yysr','yysr_g','jlr','jlr_g','mgjzc','roe']
    
    df = pd.DataFrame(data,columns=cols)
    df = df.set_index('gpdm')

    dbcn.close()    
    df['yysr_1']=np.nan
    df['jlr_1']=np.nan
    df['yjbg']='报表'
    cols = ['eps','yysr','yysr_1','yysr_g','jlr','jlr_1','jlr_g','mgjzc','roe','yjbg']
    
    return df[cols]

##########################################################################
#查询股票申万行业代码与名称
##########################################################################
def get_swhy():
    dbfn=getdrive()+'\\hyb\\STOCKHY.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()

    sql = '''select a.gpdm,a.FLDM,b.flmc from gpfldm as a,fldm as b where a.zldm='SW' and a.FLDM=b.FLDM
            ;'''
    
    curs.execute(sql)
    
    data = curs.fetchall()
    cols = ['gpdm','swhydm','swhymc']
    
    df = pd.DataFrame(data,columns=cols)
    df = df.set_index('gpdm')

    dbcn.close()    

    return df

##########################################################################
#业绩预约披露日期数据
##########################################################################
def get_yyrq():
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()

    sql = 'select gpdm,yyrq from yysj;'
    
    curs.execute(sql)
    
    data = curs.fetchall()
    cols = ['gpdm','yyrq']
    
    df = pd.DataFrame(data,columns=cols)
    df = df.set_index('gpdm')

    dbcn.close()    

    return df


##########################################################################
#查询股票商誉占比
##########################################################################
def get_syzb():
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()

    sql = '''select gpdm,syjzczb,syzzczb from gpnb where rq='2017.12.31'
            ;'''
    
    curs.execute(sql)
    
    data = curs.fetchall()
    cols = ['gpdm','syjzczb','syzzczb']
    
    df = pd.DataFrame(data,columns=cols)
    df = df.set_index('gpdm')

    dbcn.close()    

    return df



##########################################################################
#将字符串转换为时间戳，不成功返回None
##########################################################################
def str2datetime(s):
    try:
        dt = dateutil.parser.parse(s)
    except:
        dt = None

    return dt

##########################################################################
#将字符串转换为时间戳，不成功返回None
##########################################################################
def str2datetime1(s):
    dt = None
    
    if ('-' in s) or ('/' in s) or ('.' in s):
        if '-' in s:
            dt=s.split('-')
        if '/' in s:
            dt=s.split('/')        
        if '.' in s:
            dt=s.split('.')        
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
#验证roe选股文章
'''http://www.sohu.com/a/158415206_473304   ''' 
##########################################################################
def yanzheng():
    gpssrq=get_ssrq()
    gpdmb=get_gpdm()
    jbxx=gpdmb.join(gpssrq)
    
    ss=gpssrq[(gpssrq['ssrq']<'20070101')]
         
    nf1=2007
    nf2=2017
    df = get_roe(nf1,nf2)

    df=df.dropna(thresh=nf2-nf1)
    
    s_std=df.std(axis=1)
    s_mean=df.mean(axis=1)
    
    df1=pd.DataFrame({'std':s_std,'mean':s_mean})
    
    df1=jbxx.join(df1)
    
    df1=df1.join(df)
    df2=df1[df1.index.isin(ss.index)]
    
    df2['mean_num']=df2['mean'].rank(ascending=0,method='min')
    df2['std_num']=df2['std'].rank(ascending=1,method='min')
    df2=df2.assign(t_num = df2['mean_num']+df2['std_num'])
    df2=df2.sort_values(by='t_num')    
    df3=df2[((df2['std_num']<=373) & (df2['mean_num']<=373))]
 
    df3.to_excel(r'd:\selestock\roe4.xlsx')
            
    

##########################################################################
#合并工作表
##########################################################################
def joinsht():
    wb = xw.Book(r'D:\selestock\selestock1.xls')

    df1 = xw.Range('A1').options(pd.DataFrame, expand='table').value
    df1.columns=[e[0] for e in df1.columns]

    cols1=df1.columns
        
    wb = xw.Book(r'D:\selestock\selestock2.xls')

    df2 = xw.Range('A1').options(pd.DataFrame, expand='table').value
    df2.columns=[e[0] for e in df2.columns]
    
    cols2 = df2.columns
    cols2 = [e for e in cols2 if not e in cols1]
    df2=df2[cols2]

    df=df1.join(df2)    
    cols = df.columns
    
    fldd=field_dic()
    colse = [fldd[e] for e in cols] 
    
    if len(colse)!=len(list(set(colse))) :
        print("请检查selestock1.xls和selestock2.xls栏目，有重复项目。工作无法继续，退出！")
        sys.exit()
        return False
    

    wb = xw.books.open(r'D:\selestock\选股分析.xlsm')
    sht = wb.sheets('selestock')
    
    sht.select()
    sht.clear_contents()    
    xw.Range('A1').value = df

    xw.books('selestock1.xls').close()
    xw.books('selestock2.xls').close()
    
    xw.books('选股分析.xlsm').save()
    
    return True
    

##########################################################################
#根据roe选股
##########################################################################
def xg_roe():    

    gpssrq=get_ssrq()
    gpdmb=get_gpdm()
    jbxx=gpdmb.join(gpssrq)
    
    ss=gpssrq[(gpssrq['ssrq']<'20130101')]
         
    nf1=2013
    nf2=2017
    df = get_roe(nf1,nf2)

    #删除数据不全的
    df=df.dropna(thresh=nf2-nf1)
    
    s_std=df.std(axis=1)
    s_mean=df.mean(axis=1)
    
    df1=pd.DataFrame({'std':s_std,'mean':s_mean})
    
    df1=jbxx.join(df1)
    
    df1=df1.join(df)
    
    #选择2013年1月1日前上市的
    df2=df1[df1.index.isin(ss.index)]

    #前25%条数
    rows=len(df2)*0.25    
    
    #排序标号
    df2=df2.assign(mean_num=df2['mean'].rank(ascending=0,method='min'))
    df2=df2.assign(std_num=df2['std'].rank(ascending=1,method='min'))

    df2=df2.assign(t_num = df2['mean_num']+df2['std_num'])
    df2=df2.sort_values(by='t_num')

    #选择roe均值最高的25%和标准差最低的25%    
    df3=df2[((df2['std_num']<=rows) & (df2['mean_num']<=rows))]

    #关联当期相对估值PE、PB
    pepb=get_pepb('2018-02-22')    
    df3=df3.join(pepb)
 
    df3.to_excel(r'd:\selestock\roe2018.xlsx')
            
##########################################################################
#生成字典
##########################################################################
def field_dic():
    
    opened = False
    for bk in xw.books:
        if bk.name == '选股分析.xlsm' :
            opened = True
            wb=bk
    if not opened :
        wb = xw.books.open(r'D:\selestock\选股分析.xlsm')

    wb.activate()    

    sht = wb.sheets('fld_dic')
    df = sht.range('A1:B1').options(pd.DataFrame, expand='down').value
    
    #将列转换成单层索引
    df.columns=[e[0] for e in df.columns]
    
    dic = df.to_dict()
    
    return dic['fld_e']
    
        
##########################################################################
#根据净利润经营现金流含量选股
##########################################################################
def xg_jlrjyxjlhl():
    gpssrq=get_ssrq()
    gpdmb=get_gpdm()
    jbxx=gpdmb.join(gpssrq)
    
    ss=gpssrq[(gpssrq['ssrq']<'20130101')]
         
    nf1=2013
    nf2=2017
    df = get_lrxjlhl(nf1,nf2)

    #删除数据不全的
    df=df.dropna(thresh=nf2-nf1)
    
    s_std=df.std(axis=1)
    s_mean=df.mean(axis=1)
    
    df1=pd.DataFrame({'std':s_std,'mean':s_mean})
    
    df1=jbxx.join(df1)
    
    df1=df1.join(df)
    
    #选择2013年1月1日前上市的
    df2=df1[df1.index.isin(ss.index)]

    #前25%条数
    rows=len(df2)*0.30    
    
    #排序标号
    df2=df2.assign(mean_num=df2['mean'].rank(ascending=0,method='min'))
    df2=df2.assign(std_num=df2['std'].rank(ascending=1,method='min'))

    df2=df2.assign(t_num = df2['mean_num']+df2['std_num'])
    df2=df2.sort_values(by='t_num')

    #选择roe均值最高的25%和标准差最低的25%    
    df3=df2[((df2['std_num']<=rows) & (df2['mean_num']<=rows))]

    #关联当期相对估值PE、PB
    pepb=get_pepb('2018-02-22')    
    df3=df3.join(pepb)
    fn = r'd:\selestock\lrxjlhl2018.xlsx'
    writer=pd.ExcelWriter(fn,engine='xlsxwriter')

    df2.to_excel(writer, sheet_name='净利润经营现金流含量')   
    df3.to_excel(writer, sheet_name='选股')   
 

##########################################################################
#获取最新股东户数及季度环比变化
#股东户数从东方财富网抓取
#每天运行eastmoney2sqlite.py
##########################################################################
def get_gdhs():

    dbfn=getdrive()+'\\hyb\\DZH.db'
    
#    dbfn=getdrive()+'\\hyb\\STOCKDATA.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    #rq0最新、rq1最新季度、rq2最新年度、rq3上一年度
#    rq1='2019-03-31'
    rq2='2018-12-31'
    rq3='2017-12-31'
    #提取年报以来最新股东户数，注意最新在前
    sql="select gpdm,rq,gdhs from gdhs where rq>='%s' order by rq desc;" % rq2    
    curs.execute(sql)
    
    data = curs.fetchall()
    
    #保留最新户数
    df0=pd.DataFrame(data,columns=['gpdm','rq0','gdhs0'])
    df0 = df0.drop_duplicates(['gpdm'],keep='first')
    
    #所有数据
    df1=pd.DataFrame(data,columns=['gpdm','rq0','gdhs0'])
    df1=df1.append(df0)
    
    '''#下一句很关键
    #Python Dataframe ——指定多列去重、求差集
    #https://blog.csdn.net/jasonwang_/article/details/78797458
    #https://blog.csdn.net/jasonwang_/article/details/78984852
    '''
    
    df1 = df1.drop_duplicates(keep=False)   #去掉重复
    
    df1 = df1.drop_duplicates(['gpdm'],keep='first')    #保留次新股东户数
    df1.columns=['gpdm','rq1','gdhs1']

    df0 = df0.set_index('gpdm')
    df1 = df1.set_index('gpdm')
        
    sql="select gpdm,rq,gdhs from gdhs where rq=='%s' order by rq desc;" % rq2    
    curs.execute(sql)

    data = curs.fetchall()
    
    df2=pd.DataFrame(data,columns=['gpdm','rq2','gdhs2'])
    df2=df2.set_index('gpdm')

    sql="select gpdm,rq,gdhs from gdhs where rq=='%s' order by rq desc;" % rq3    
    curs.execute(sql)

    data = curs.fetchall()

    dbcn.close()    
    
    df3=pd.DataFrame(data,columns=['gpdm','rq3','gdhs3'])
    df3=df3.set_index('gpdm')
    
    df=df0.join(df1)
    df=df.join(df2)
    df=df.join(df3)
    
    #股东户数近期比较，年度同比、季度环比变化
    df.eval('gdhsjb = (gdhs0 / gdhs1-1) * 100',inplace=True)
    df.eval('gdhshb = (gdhs0 / gdhs2-1) * 100',inplace=True)
    df.eval('gdhstb = (gdhs0 / gdhs3-1) * 100',inplace=True)
    
    #将没有最新股东户数信息的设为NAN
#    df.loc[df['rq0']==df['rq1'],'gdhsjb']=np.nan
#    df.loc[df['rq1']==df['rq2'],'gdhshb']=np.nan
    #注意：在进行df3=df1.join(df2)操作时，缺失值为np.nan，会将gphs2列数值类型设为float64
    #不再是原来的int64
    return df.round(2)    



########################################################################
#提取质押率
########################################################################
def get_zyl():

    '''
    读取i问财下载的xls文件：
    公司出质人累计质押比例合计(%) 公司出质人质押占总股本比合计(%)

    '''
    
    xlsfn = 'd:\\selestock\\zyl.xls'
    coldic={'股票代码':'gpdm',	
        '公司出质人累计质押比例合计(%)'	:'zyl',
        '公司出质人质押占总股本比合计(%)':'zyzgbzb'
        }
    
    colfloat=['zyl','zyzgbzb']
    colsele=['gpdm','zyl','zyzgbzb']
    keycol='gpdm'

    df=xls2pd(xlsfn,coldic,colfloat,colsele)

    df=df[colsele]

    df=df.set_index(keycol)
    
    return df
    
    
########################################################################
#提取带息负债率
########################################################################
def get_dxfzl():

    '''
    读取i问财下载的xls文件：
    
    带息负债/资产总计

    '''
    
    xlsfn = 'd:\\selestock\\fzl_dxfz.xls'
    coldic={'股票代码':'gpdm',	
        '[1]/[2]'	:'dxfzl'
        }
    
    colfloat=['dxfzl']
    colsele=['gpdm','dxfzl']
    keycol='gpdm'

    df=xls2pd(xlsfn,coldic,colfloat,colsele)

    df=df[colsele]
    df['dxfzl']=df['dxfzl']*100

    df=df.set_index(keycol)

    
    return df
    

########################################################################
#前十大流通股东占流通股比例
########################################################################
def get_dgdltzb(xlsfn):

    '''
    读取i问财下载的xls文件：

    '''
    
#    xlsfn = 'd:\\selestock\\ABH2018_1lt.xls'
    coldic={'股票代码':'gpdm',	
        '流通a股(股)'	:'ag',
        '流通b股(股)':'bg',
        '香港上市股(股)':'hg',
        '十大流通股东持股数(股)':'dgdcg'
        }
    
    colfloat=['ag','bg','hg','dgdcg']
    colsele=['gpdm','ag','bg','hg','dgdcg']
    keycol='gpdm'

    df=xls2pd(xlsfn,coldic,colfloat,colsele)

    df=df[colsele]
    df=df.fillna({'ag':0,'bg':0,'hg':0})
    df=df.assign(dgdltzb=df['dgdcg']/(df['ag']+df['bg']+df['hg']))

    df=df.set_index(keycol)

    
    return df[['dgdltzb']]
 
    
########################################################################
#最新流通股数量
########################################################################
def get_ltg():

    '''
    读取i问财下载的xls文件：
    流通a股 流通b股 香港上市股份

    '''
    
    xlsfn = 'd:\\selestock\\ABlt.xls'
    coldic={'股票代码':'gpdm',	
        '流通a股(股)'	:'ag',
        '流通b股(股)':'bg'
        }
    
    colfloat=['ag','bg']
    colsele=['gpdm','ag','bg']
    keycol='gpdm'

    df=xls2pd(xlsfn,coldic,colfloat,colsele)

    df=df[colsele]
    df=df.fillna({'ag':0,'bg':0})
    df=df.assign(zxltg=df['ag'])

    df=df.set_index(keycol)

    
    return df[['zxltg']]


########################################################################
#最新流通股数量
########################################################################
def get_hltg():

    '''
    读取i问财下载的xls文件：
    香港上市股份

    '''
    
    xlsfn = 'd:\\selestock\\Hlt.xls'
    coldic={'股票代码':'gpdm',	
        '流通a股(股)'	:'ag',
        '流通b股(股)':'bg',
        '香港上市股(股)':'hg'
        }
    
    colfloat=['ag','bg','hg']
    colsele=['gpdm','ag','bg','hg']
    keycol='gpdm'

    df=xls2pd(xlsfn,coldic,colfloat,colsele)

    df=df[colsele]
    df=df.fillna({'ag':0,'bg':0,'hg':0})
    df=df.assign(zxltg=df['ag'])

    df=df.set_index(keycol)

    
    return df[['zxltg']]

########################################################################
#最新除权除息日期
########################################################################
def get_cqcxrq():

    '''
    读取i问财下载的xls文件：
    最新除权除息日期

    '''
    
    xlsfn = 'd:\\selestock\\cqcx.xls'
    coldic={'股票代码':'gpdm',	
        '除权除息日':'cqcxr'
        }
    
    colfloat=[]
    colsele=['gpdm','cqcxr']
    keycol='gpdm'

    df=xls2pd(xlsfn,coldic,colfloat,colsele)

    df=df[colsele]
    df=df.set_index(keycol)
    
    cqcxrq={}
    for k,v in df.iterrows():
        if k[0] in ('0','3','6') and v[0]:
            cqcxrq[k]=dateutil.parser.parse(v[0]).strftime("%Y-%m-%d")

    return cqcxrq



########################################################################
#流通大股东占比，
########################################################################
def get_dzh_dgdltzb():

    dbfn=getdrive()+'\\hyb\\STOCKDATA.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    
    #提取年报以来最新股东户数    
    curs.execute('''select gpdm,rq,ltg,sdgdzb,shcg from ltg 
               order by rq desc;''')
    
    data = curs.fetchall()
    dbcn.close()
    
    #保留最新户数
    df=pd.DataFrame(data,columns=['gpdm','ltgrq','ltg','dgdltzb','shcg'])
    df = df.drop_duplicates(['gpdm'],keep='first')
    df = df.set_index('gpdm')

    df['ltg'] = df['ltg']/10000
    df['shcg'] = df['shcg']*10000
    
    return df.round(2)    


########################################################################
#流通大股东占比，
########################################################################
def get_dfcf_dgdltzb():

    dbfn=getdrive()+'\\hyb\\STOCKDATA.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    
    #提取年报以来最新股东户数    
    curs.execute('''select gpdm,rq,ltg,sdgdzb,shcg,dgdgs from ltg 
               order by rq desc;''')
    
    data = curs.fetchall()
    dbcn.close()
    
    #保留最新户数
    df=pd.DataFrame(data,columns=['gpdm','ltgrq','ltg','dgdltzb','shcg','dgdgs'])
    df = df.drop_duplicates(['gpdm'],keep='first')
    df = df.set_index('gpdm')

    return df.round(2)    


########################################################################
#从大智慧读取的数据，大股东流通占比，从问财下载最新a流通股
########################################################################
def get_shcg():

    df=get_ltg()    
    dgdltzb=get_dfcf_dgdltzb()
    
    df=df.join(dgdltzb)
    
    df['shcg']=df['zxltg']*(1-df['dgdltzb']/100)

    df.loc[df['shcg']<=0,'shcg']=np.nan

    df['zxltg']=df['zxltg']/100000000

    return df.round(2)

########################################################################
#从大智慧读取的数据，大股东流通占比
########################################################################
def get_shcg3():

    dbfn=getdrive()+'\\hyb\\STOCKDATA.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    
    #提取年报以来最新股东户数    
    curs.execute('''select gpdm,rq,ltg,sdgdzb,shcg from ltg 
               order by rq desc;''')
    
    data = curs.fetchall()
    dbcn.close()
    
    #保留最新户数
    df=pd.DataFrame(data,columns=['gpdm','ltgrq','ltg','dgdltzb','shcg'])
    df = df.drop_duplicates(['gpdm'],keep='first')
    df = df.set_index('gpdm')

    df['ltg'] = df['ltg']/10000
    df['shcg'] = df['shcg']*10000
    
    return df.round(2)    


    

########################################################################
#散户持股：B股、H股为全流通，
########################################################################
def get_shcg2():

    '''
    读取i问财下载的xls文件：
    检索：
    
    1、2018年3月31日流通a股 2018年3月31日流通b股 2018年3月31日香港上市股份 2018年3月31日前十大流通股东持股

    2、2018年6月30日流通a股 2018年6月30日流通b股 2018年6月30日香港上市股份 2018年6月30日前十大流通股东持股

    3、流通a股 流通b股 香港上市股份 


    选择：流通a股(股),十大流通股东持股数 (股),流通b股(股),香港上市股(股)
	
    特别提醒：对于近期有大幅解禁的，该数据不准确。因为流通A股是最新值，而前十大流通股东占比不是最新值
    
    '''
    
    #2018年一季报前十大股东流通股占总流通股比例
    xlsfn = 'd:\\selestock\\ABH2018_1lt.xls'
    df1=get_dgdltzb(xlsfn)
    
    #2018年二季报前十大股东流通股占总流通股比例
    xlsfn = 'd:\\selestock\\ABH2018_2lt.xls'
    df2=get_dgdltzb(xlsfn)
    
    #有2季报占比的用哦季报占比，没有的用一季报占比
    df=df1.join(df2,lsuffix='1',rsuffix='2')
    df=df.assign(dgdltzb=df['dgdltzb2'])
    df.loc[df['dgdltzb'].isna(),'dgdltzb']=df['dgdltzb1']

    ltg=get_ltg()
    ltg=ltg.join(df)
    
    #用最新季报前十大流通股东持股占比计算除前十大流通股东外散户持股总数
    ltg=ltg.assign(shcg=ltg['ltg']*(1-ltg['dgdltzb']))
    ltg['ltg']=ltg['ltg']/100000000
    ltg['dgdltzb']=ltg['dgdltzb']*100
    
    return ltg[['ltg','dgdltzb','shcg']]
   
########################################################################
#散户持股：B股、H股为全流通，
########################################################################
def get_shcg1():

    '''
    读取i问财下载的xls文件：
    检索：流通A股数 或 流通B股数 或 H股数 或 2018.06.30前十大流通股东合计持股数 或 2018.03.31前十大流通股东合计持股数    

    2018年3月31日流通a股 2018年3月31日流通b股 2018年3月31日香港上市股份 2018年3月31日前十大流通股东持股

    2018年6月30日流通a股 2018年6月30日流通b股 2018年6月30日香港上市股份 2018年6月30日前十大流通股东持股


    流通a股 流通b股 香港上市股份 


    选择：流通a股(股),十大流通股东持股数 (股),流通b股(股),香港上市股(股)
	
    特别提醒：对于近期有大幅解禁的，该数据不准确。因为流通A股是最新值，而前十大流通股东合计不是最新值
    
    
    coldic中含有“_”表示前缀
    '''
    
    bgq = {'.03.31':'_1','.06.30':'_2','.09.30':'_3','.12.31':'_4'}
    
    xlsfn = 'd:\\selestock\\ABH.xls'
    coldic={'股票代码':'gpdm',	
        '股票简称':'gpmc',    
        '流通a股(股)':'ag',
        '流通b股(股)':'bg',
        '流通b股(股)':'bg',
        '香港上市股(股)':'hg',
        '十大流通股东持股比例(%)':'dgdcgbl_'
        }
    
    colfloat=['ag','bg','hg','dgdcgbl_']
    keycol='gpdm'
    coldel=['gpmc']
    
    with open(xlsfn,'rb') as f:
        content = f.read().decode('utf8','ignore')
        f.close()

    soup = bs(content,"lxml")
    tables = soup.findAll('table')
    tab = tables[0]
    data=[]
    for tr in tab.findAll('tr'):
        row=[]
        tds=tr.findAll('td')
        for i in range(len(tds)):
            content=tds[i].getText().replace('\t','').replace('\r','').replace('--','0')
            if len(content)==0:
                content=None
            row.append(content)
        data.append(row)

    cols=data[0]
    data=data[1:]
    df = pd.DataFrame(data,columns=cols)
    
    #第一次变换非前缀型字段
    sele=[]
    flds=[]
    for i in range(len(df.columns)):
        fld=df.columns[i]
        if fld in coldic.keys():
            flds.append(coldic[fld])
            sele.append(1)
        else:
            flds.append(fld)
            sele.append(0)

    #第二次变化前缀型字段            
    for k in coldic.keys():
        if '_' in coldic[k]:
            prefix=coldic[k]
            for i in range(len(flds)):
                col = flds[i]
                if k in col:
                    col=col.replace(k,prefix)
                    for bgqrq in bgq.keys():
                        if bgqrq in col:
                            flds[i]=col.replace(bgqrq,bgq[bgqrq])
                        
                    sele[i]=1


    #第三次变化,删除日期            
    for i in range(len(flds)):
        flds[i] = re.sub('\d{4}\.\d{2}\.\d{2}','',flds[i])
        flds[i] = re.sub('-','',flds[i])
        flds[i].replace(' ','')


    #第四次变化,            
    for i in range(len(df.columns)):
        fld=flds[i]
        if fld in coldic.keys():
            flds[i]=coldic[fld]
            sele[i]=1
                    
    #修改字段名：注意字段名的顺序不能变            
    df.columns=flds
    
    #提取有用字段
    flds1=[]
    for i in range(len(sele)):
        if sele[i]:
            flds1.append(flds[i])

    df=df[flds1] 
    
    #去掉指定列
    if len(coldel)>0:
        df=df.drop(columns=coldel)       
    
    #转换成数值
    flds=[]
    for i in range(len(df.columns)):
        fld=df.columns[i]
        for j in range(len(colfloat)):
            if colfloat[j] in fld:
                flds.append(fld)
                
    for fld in flds:
        df[fld]=df[fld].map(str2float_none)
    
    df=df.set_index(keycol)    
    
    df.loc[df['dgdcgbl_2018_2']==0,'dgdcgbl_2018_2']=df['dgdcgbl_2018_1']
    
    df=df.assign(shcg=(df['ag']+df['bg']+df['hg'])*(1-df['dgdcgbl_2018_2']/100))
    
    df=df.loc[df['shcg']>0]
    
    return df[['shcg']]
    
    
########################################################################
#提取负债率
########################################################################
def get_fzl():

    '''
    读取i问财下载的xls文件：
    负债率
    
    coldic中含有“_”表示前缀
    '''
    
    xlsfn = 'd:\\selestock\\fzl.xls'
    coldic={'股票代码':'gpdm',	
        '股票简称':'gpmc',	
        '资产负债率(%)'	:'fzl_'
        }
    
    colfloat=['fzl_']
    keycol='gpdm'
    coldel=['gpmc']
    
    with open(xlsfn,'rb') as f:
        content = f.read().decode('utf8','ignore')
        f.close()

    soup = bs(content,"lxml")
    tables = soup.findAll('table')
    tab = tables[0]
    data=[]
    for tr in tab.findAll('tr'):
        row=[]
        tds=tr.findAll('td')
        for i in range(len(tds)):
            content=tds[i].getText().replace('\t','').replace('\r','').replace('--','')
            if len(content)==0:
                content=None
            row.append(content)
        data.append(row)

    cols=data[0]
    data=data[1:]
    df = pd.DataFrame(data,columns=cols)
    
    #第一次变换非前缀型字段
    sele=[]
    flds=[]
    for i in range(len(df.columns)):
        fld=df.columns[i]
        if fld in coldic.keys():
            flds.append(coldic[fld])
            sele.append(1)
        else:
            flds.append(fld)
            sele.append(0)

    #第二次变化前缀型字段            
    for k in coldic.keys():
        if '_' in coldic[k]:
            prefix=coldic[k]
            for i in range(len(flds)):
                col = flds[i]
                if k in col:
                    flds[i]=col.replace(k,prefix)
                    sele[i]=1
                    
    #修改字段名：注意字段名的顺序不能变            
    df.columns=flds
    
    #提取有用字段
    flds1=[]
    for i in range(len(sele)):
        if sele[i]:
            flds1.append(flds[i])

    df=df[flds1] 
    
    #去掉指定列
    if len(coldel)>0:
        df=df.drop(columns=coldel)       
    
    #转换成数值
    flds=[]
    for i in range(len(df.columns)):
        fld=df.columns[i]
        for j in range(len(colfloat)):
            if colfloat[j] in fld:
                flds.append(fld)
                
    for fld in flds:
        df[fld]=df[fld].map(str2float_none)
    
    df=df.set_index(keycol)    
    return df
    
##########################################################################
#日期字符串，指定日期后days天，days为负表示前days天
##########################################################################
def strnextdate(date,days):
    
    return (str2datetime(date) + datetime.timedelta(days)).strftime("%Y%m%d")
    
    

##########################################################################
#获取股票基本信息：代码、名称、拼音、行业、商誉占比、股东户数及变化、当前股价
##########################################################################
def get_jbxx():
    
    stkinfo=get_stock_basics()
    stkinfo['gpdm']=stkinfo.index.map(lambda x:x+('.SH' if x[0]=='6' else '.SZ'))
    stkinfo['ssrq']=stkinfo['timeToMarket'].map(lambda x:str(x) if x>0 else '')
    stkinfo['mgjzc']=stkinfo['bvps']
    stkinfo['zgb']=stkinfo['totals']
    stkinfo['zjhhy']=stkinfo['industry']
    gpssrq=stkinfo[['gpdm','ssrq','zjhhy','zgb','mgjzc']]
    gpssrq=gpssrq.set_index('gpdm')
    
#    gpssrq=get_ssrq()
    #股票代码、名称、拼音
    gpdmb=get_gpdm()
    
    #股票所属通达信概念风格指数,重要提示
    gptdxgfz=get_zyts()
    
    #股票申万行业
    gpswhy=get_swhy()
    #股票商誉占比
    gpsyzb=get_syzb()
    
    #最新股东户数
    gpgdhs=get_gdhs()
    
    #最近5年yysr
    gpyysr=yysr_nf_n(2017,5)
    
    #最近5年jlr
    gpjlr=jlr_nf_n(2017,5)
    
    #最近5年roe
    gproe=roe_nf_n(2017,5)
    
    #最近5年eps0
    gpeps=eps_nf_n(2017,5)

    #最近5年eps0_g
    gpepsg=epsg_nf_n(2017,5)

    #最近5年roa
    gproa=roa_nf_n(2017,5)

    #最近5年jll
    gpjll=jll_nf_n(2017,5)

    #最近5年jll
    gpmll=mll_nf_n(2017,5)

    #最近5年srzyxjhl1
    gpsrzyxjhl1=srzyxjhl1_nf_n(2017,5)

    #最近5年srzyxjhl2
    gpsrzyxjhl2=srzyxjhl2_nf_n(2017,5)

    #业绩预约披露日期
    yyrq=get_yyrq()

    #最新业绩
    yjyg=get_zxyj_bgq('2018-06-30')
    
    yjyg1=get_yjyg_bgq('2018-09-30')
    
    yjyg=yjyg.join(yjyg1,rsuffix='3')
    
    #负债率
    gpfzl=get_fzl()
    gpdxfzl=get_dxfzl()
    
    #质押率
    gpzyl=get_zyl()
    
    #最新散户持股
    xsjj=dfjj()

    #最新散户持股
    shcg=get_shcg()

    #最新散户持股
    szgbl=get_szgbl()

    #读取最新中证行业和PE、PB信息
    gppepb=get_pepb_zzhy()

    #信息合并
    jbxx=gpdmb.join(gpssrq)
    jbxx=jbxx.join(gpswhy)
    jbxx=jbxx.join(gppepb)
    jbxx=jbxx.join(gptdxgfz)
    jbxx=jbxx.join(gpsyzb)
    jbxx=jbxx.join(gpgdhs)
    jbxx=jbxx.join(shcg)
    jbxx=jbxx.join(gpyysr)
    jbxx=jbxx.join(gpjlr)
    jbxx=jbxx.join(gproe)
    jbxx=jbxx.join(gpeps)
    jbxx=jbxx.join(gpepsg)
    jbxx=jbxx.join(gproa)
    jbxx=jbxx.join(gpjll)
    jbxx=jbxx.join(gpmll)
    jbxx=jbxx.join(gpsrzyxjhl1)
    jbxx=jbxx.join(gpsrzyxjhl2)
    jbxx=jbxx.join(gpfzl)
    jbxx=jbxx.join(gpdxfzl)
    jbxx=jbxx.join(gpzyl)
    jbxx=jbxx.join(yyrq)
    jbxx=jbxx.join(yjyg)


    #提取股价
    gpgj=get_todaygj()
    
    #信息合并
    jbxx=jbxx.join(gpgj)
    #散户户均持股数，户均市值，这里把前十大流通股东外的均称为散户（万股，万元)
    jbxx=jbxx.assign(hjcg=jbxx['shcg']/(jbxx['gdhs0']-jbxx['dgdgs'])/10000)
    jbxx=jbxx.assign(hjsz=jbxx['hjcg']*jbxx['gj'])
    jbxx=jbxx.drop(columns=['shcg'])

    jbxx=jbxx.join(xsjj)
    jbxx=jbxx.join(szgbl)
    
    jbxx=jbxx.fillna(value={'zyts':'','yj':'','yj3':'','szgbl':1})

    return jbxx    


##########################################################################
#nf年前n年roa
##########################################################################
def roa_nf_n(nf,n):

    for i in range(nf,nf-n,-1):
        r=query_roa(i)
        if i==nf :
            df=r
        else:
            df=df.join(r)
            
    return df

##########################################################################
#nf年前n年jll
##########################################################################
def jll_nf_n(nf,n):

    for i in range(nf,nf-n,-1):
        r=query_jll(i)
        if i==nf :
            df=r
        else:
            df=df.join(r)
            
    return df


##########################################################################
#nf年前n年jll
##########################################################################
def mll_nf_n(nf,n):

    for i in range(nf,nf-n,-1):
        r=query_mll(i)
        if i==nf :
            df=r
        else:
            df=df.join(r)
            
    return df
##########################################################################
#nf年前n年srzyxjhl1
##########################################################################
def srzyxjhl1_nf_n(nf,n):

    for i in range(nf,nf-n,-1):
        r=query_srzyxjhl1(i)
        if i==nf :
            df=r
        else:
            df=df.join(r)
            
    return df

##########################################################################
#nf年前n年srzyxjhl2
##########################################################################
def srzyxjhl2_nf_n(nf,n):

    for i in range(nf,nf-n,-1):
        r=query_srzyxjhl2(i)
        if i==nf :
            df=r
        else:
            df=df.join(r)
            
    return df

##########################################################################
#nf年前n年yysr
##########################################################################
def yysr_nf_n(nf,n):

    for i in range(nf,nf-n,-1):
        r=query_yysr(i)
        if i==nf :
            df=r
        else:
            df=df.join(r)
            
    return df

##########################################################################
#nf年前n年jlr
##########################################################################
def jlr_nf_n(nf,n):

    for i in range(nf,nf-n,-1):
        r=query_jlr(i)
        if i==nf :
            df=r
        else:
            df=df.join(r)
            
    return df

##########################################################################
#nf年前n年roe
##########################################################################
def roe_nf_n(nf,n):

    for i in range(nf,nf-n,-1):
        r=query_roe(i)
        if i==nf :
            df=r
        else:
            df=df.join(r)
            
    return df

##########################################################################
#nf年前n年roe
##########################################################################
def eps_nf_n(nf,n):

    for i in range(nf,nf-n,-1):
        r=query_eps(i)
        if i==nf :
            df=r
        else:
            df=df.join(r)
            
    return df

##########################################################################
#nf年前n年roe
##########################################################################
def epsg_nf_n(nf,n):

    for i in range(nf,nf-n,-1):
        r=query_epsg(i)
        if i==nf :
            df=r
        else:
            df=df.join(r)
            
    return df

##########################################################################
#获取2017年业绩（包括报表、快报和预告）
##########################################################################
def get_yj2017():
    nf=2017
    yjyg=get_yjyg(nf)    
    
    yjbb=get_yjbb(nf)
    
    yjkb=get_yjkb(nf)
    
    yj=pd.concat([yjbb,yjkb,yjyg])
    
    yj=yj[~yj.index.duplicated()]
    
    yj=yj.replace('-',np.nan) 
    
    return yj

##########################################################################
#获取研究机构对2018年盈利预测的EPS均值、最高、最低，研究就够至少2家
##########################################################################
def get_ylyc2018():
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    cn = sqlite3.connect(dbfn)
    curs = cn.cursor()
#    rq = (datetime.datetime.now()-datetime.timedelta(183)).strftime('%Y-%m-%d')
    rq = '2017-12-01'
    sql='select gpdm,rq,pjjg,eps2 from report where rq>=?;'
    curs.execute(sql,(rq,))        
    data = curs.fetchall()

    cols = ['gpdm','rq','pjjg','eps1']
    
    df=pd.DataFrame(data,columns=cols)
    
    #按日期降序排列，最新研报排在前面
    df=df.sort_values(by='rq',ascending=False)
    
    #去掉同一评级机构以前的研报
    df=df.drop_duplicates(['gpdm','pjjg'],keep='first')
    
    #按股票分组
    grouped=df.groupby('gpdm')

    #统计每只股票的eps预测评级机构家数，均值，标准差，最大值，最小值
    epsdf=grouped['eps1'].agg([np.size, np.mean, np.std, np.max, np.min])
    #返回评级机构超过2家的股票
    return epsdf.loc[(epsdf['size']>=1)]

##########################################################################
#获取研究机构对2018年盈利预测的EPS均值、最高、最低，研究就够至少2家
##########################################################################
def get_ylyc2018_dzh():
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    cn = sqlite3.connect(dbfn)
    curs = cn.cursor()
    rq = (datetime.datetime.now()-datetime.timedelta(183)).strftime('%Y-%m-%d')
#    rq = '2017-12-01'
#    sql='select gpdm,rq,pjjg,eps2 from dzhrpt where rq>=?;'
    sql = '''select gpdm,rq,pjjg,eps from (
             select distinct gpdm,rq,pjjg,eps1 as eps from dzhrpt where nf1='2018预测' and eps1 is not null
             union select distinct gpdm,rq,pjjg,eps2 as eps from dzhrpt where nf2='2018预测' and eps2 is not null) 
             where rq>=?
            '''

    curs.execute(sql,(rq,))        
    data = curs.fetchall()

    cols = ['gpdm','rq','pjjg','eps1']
    
    df=pd.DataFrame(data,columns=cols)
    
    #按日期降序排列，最新研报排在前面
    df=df.sort_values(by='rq',ascending=False)
    
    #去掉同一评级机构以前的研报
    df=df.drop_duplicates(['gpdm','pjjg'],keep='first')
    
    #按股票分组
    grouped=df.groupby('gpdm')

    #统计每只股票的eps预测评级机构家数，均值，标准差，最大值，最小值
    epsdf=grouped['eps1'].agg([np.size, np.mean, np.std, np.max, np.min])
    #返回评级机构超过2家的股票
    return epsdf.loc[(epsdf['size']>=1)]

##########################################################################
#获取研究机构对2018年盈利预测的EPS均值、最高、最低，研究就够至少2家
##########################################################################
def get_ylyc2018_gao():
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    cn = sqlite3.connect(dbfn)
    curs = cn.cursor()
    rq = (datetime.datetime.now()-datetime.timedelta(183)).strftime('%Y-%m-%d')
#    rq = '2017-12-01'

    sql = '''select gpdm,rq,pjjg,eps from gaorpt  
             where nf='2018' and rq>=?
            '''

    curs.execute(sql,(rq,))        
    data = curs.fetchall()

    cols = ['gpdm','rq','pjjg','eps1']
    
    df=pd.DataFrame(data,columns=cols)
    
    #按日期降序排列，最新研报排在前面
    df=df.sort_values(by='rq',ascending=False)
    
    #去掉同一评级机构以前的研报
    df=df.drop_duplicates(['gpdm','pjjg'],keep='first')
    
    #按股票分组
    grouped=df.groupby('gpdm')

    #统计每只股票的eps预测评级机构家数，均值，标准差，最大值，最小值
    epsdf=grouped['eps1'].agg([np.size, np.mean, np.std, np.max, np.min])
    #返回评级机构超过2家的股票
    return epsdf.loc[(epsdf['size']>=1)]

##########################################################################
#获取研究机构对2018年盈利预测的EPS均值、最高、最低，研究就够至少2家
##########################################################################
def get_ylyc2018_cfi():
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    cn = sqlite3.connect(dbfn)
    curs = cn.cursor()
    rq = (datetime.datetime.now()-datetime.timedelta(183)).strftime('%Y-%m-%d')
#    rq = '2017-12-01'

    sql = '''select gpdm,rq,jg,eps from gaorpt  
             where nf='2018' and rq>=?
            '''

    curs.execute(sql,(rq,))        
    data = curs.fetchall()

    cols = ['gpdm','rq','pjjg','eps1']
    
    df=pd.DataFrame(data,columns=cols)
    
    #按日期降序排列，最新研报排在前面
    df=df.sort_values(by='rq',ascending=False)
    
    #去掉同一评级机构以前的研报
    df=df.drop_duplicates(['gpdm','pjjg'],keep='first')
    
    #按股票分组
    grouped=df.groupby('gpdm')

    #统计每只股票的eps预测评级机构家数，均值，标准差，最大值，最小值
    epsdf=grouped['eps1'].agg([np.size, np.mean, np.std, np.max, np.min])
    #返回评级机构超过2家的股票
    return epsdf.loc[(epsdf['size']>=1)]


##########################################################################
#评级机构名称统一化
##########################################################################
def getpjjg(): 

    csvfn=getdrive()+'\\selestock\\pjjgmc.csv'
    with open(csvfn,'rb') as f:
        content = f.read().decode('GBK','ignore')
        f.close()    
    
    data=content.split('\r\n')
    data=[e.split(',') for e in data]
    cols=data[0]
    data=data[1:]
    df=pd.DataFrame(data,columns=cols)
    df=df.dropna()
    df=df.set_index('pjjg',drop=False)
    df.index.name=''    
#    xlsfn=getdrive()+'\\hyb\\pjjgmc.xlsx'
#    wb = xw.Book(xlsfn)
#
#    #读取数据
#    data = wb.sheets[0].range('A1').options(pd.DataFrame, expand='table').value
#
#    '''下面的语句很重要，MultiIndex转换成Index'''
#    data.columns=[e[0] for e in data.columns]
#    data['pjjg']=data.index
#    
#    xw.apps[0].quit()
#    return data

    return df

##########################################################################
#获取研究机构对2018年盈利预测的EPS均值、最高、最低，研究就够至少2家
##########################################################################
def ylyc2018():
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    cn = sqlite3.connect(dbfn)
    curs = cn.cursor()
    rq = (datetime.datetime.now()-datetime.timedelta(183)).strftime('%Y-%m-%d')
#    rq = '2017-12-01'

    sql = '''select distinct gpdm,rq,pjjg,eps from (
             select distinct gpdm,rq,jg as pjjg,eps from cfi_yb 
             where nf='2018' 
             union select gpdm,rq,pjjg,eps from gaorpt  
             where nf='2018')
             where rq>=?
            '''

    curs.execute(sql,(rq,))        
    data = curs.fetchall()

    cols = ['gpdm','rq','pjjg','eps1']
    
    df1=pd.DataFrame(data,columns=cols)

    sql = '''select distinct gpdm,rq,pjjg,eps from (
             select distinct gpdm,rq,pjjg,eps1 as eps from report 
             where nf1='2018预测' and eps1 is not null
             union select distinct gpdm,rq,pjjg,eps2 as eps from report 
             where nf2='2018预测' and eps2 is not null
             ) 
             where rq>=?
            '''
    curs.execute(sql,(rq,))        
    data = curs.fetchall()
    
    df2=pd.DataFrame(data,columns=cols)

    sql = '''select gpdm,rq,pjjg,eps from (
             select distinct gpdm,rq,pjjg,eps1 as eps from dzhrpt 
             where nf1='2018预测' and eps1 is not null
             union select distinct gpdm,rq,pjjg,eps2 as eps from dzhrpt 
             where nf2='2018预测' and eps2 is not null
             ) 
             where rq>=?
            '''

    curs.execute(sql,(rq,))        
    data = curs.fetchall()
    
    df3=pd.DataFrame(data,columns=cols)
    
    #合并
    df=pd.concat([df1,df2,df3])
    
    df0=getpjjg()
    
    df=pd.merge(df,df0)

    #按日期降序排列，最新研报排在前面
    df=df.sort_values(by='rq',ascending=False)
    
    #去掉同一评级机构以前的研报
    df1=df.drop_duplicates(['gpdm','jgmc'],keep='first')
    
    #按股票分组
    grouped=df1.groupby('gpdm')

    #统计每只股票的eps预测评级机构家数，均值，标准差，最大值，最小值
    epsdf=grouped['eps1'].agg([np.size, np.mean, np.std, np.max, np.min])
    #返回评级机构超过2家的股票
    return epsdf.loc[(epsdf['size']>=2)]


##########################################################################
#获取研究机构对2018年盈利预测的EPS均值、最高、最低,保存为XLS文件
##########################################################################
def save_2018ylyc():
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    cn = sqlite3.connect(dbfn)
    curs = cn.cursor()
    rq = (datetime.datetime.now()-datetime.timedelta(183)).strftime('%Y-%m-%d')
#    rq = '2017-12-01'

    sql = '''select gpdm,rq,pjjg,eps,ly from (
             select distinct gpdm,rq,jg as pjjg,eps,"cfi" as ly from cfi_yb 
             where nf='2018'
             union select gpdm,rq,pjjg,eps,"gao" as ly from gaorpt  
             where nf='2018')
             where rq>=?
            '''

    curs.execute(sql,(rq,))        
    data = curs.fetchall()

    cols = ['gpdm','rq','pjjg','eps1','ly']
    
    df1=pd.DataFrame(data,columns=cols)

    sql = '''select gpdm,rq,pjjg,eps,ly from (
             select distinct gpdm,rq,pjjg,eps1 as eps,"dfcf" as ly from report 
             where nf1='2018预测' and eps1 is not null
             union select distinct gpdm,rq,pjjg,eps2 as eps,"dfcf" as ly from report 
             where nf2='2018预测' and eps2 is not null
             ) 
             where rq>=?
            '''
    curs.execute(sql,(rq,))        
    data = curs.fetchall()
    
    df2=pd.DataFrame(data,columns=cols)

    sql = '''select gpdm,rq,pjjg,eps,ly from (
             select distinct gpdm,rq,pjjg,eps1 as eps,"dzh" as ly from dzhrpt 
             where nf1='2018预测' and eps1 is not null
             union select distinct gpdm,rq,pjjg,eps2 as eps,"dzh" as ly from dzhrpt 
             where nf2='2018预测' and eps2 is not null
             ) 
             where rq>=?
            '''

    curs.execute(sql,(rq,))        
    data = curs.fetchall()
    
    df3=pd.DataFrame(data,columns=cols)
    
    #合并
    df=pd.concat([df1,df2,df3])
    
    
    #评级机构名称统一化
    df0=getpjjg()
    df=pd.merge(df,df0)

    #按日期降序排列，最新研报排在前面
    df=df.sort_values(by='rq',ascending=False)
    
    #去掉同一评级机构以前的研报
    df1=df.drop_duplicates(['gpdm','jgmc'],keep='first')
        
    #按股票分组
    grouped=df1.groupby('gpdm')

    #统计每只股票的eps预测评级机构家数，均值，标准差，最大值，最小值
    epsdf=grouped['eps1'].agg([np.size, np.mean, np.std, np.max, np.min])

    now = datetime.datetime.now().strftime('%Y%m%d_%H%M')
    fn = r'd:\selestock\eps2018_'+now+'.xlsx'

    if os.path.exists(fn):
        os.remove(fn)

    writer=pd.ExcelWriter(fn,engine='xlsxwriter')

    df.to_excel(writer, sheet_name='所有业绩预测')   
    df1.to_excel(writer, sheet_name='最新业绩预测')  
    epsdf.to_excel(writer, sheet_name='业绩预测统计')  

    writer.save()

    return

##########################################################################
#生成日期ttm包含的4个季度日期列表
##########################################################################
def rqttm(nf,jd):
    rqlst=['.03.31','.06.30','.09.30','.12.31']
    if nf <1991 or nf >2020 :
        return None
        
    if jd not in (1,2,3,4) :
        return None
    
    rq=[]
    for i in range(jd-1,-1,-1):
        rq.append(str(nf)+rqlst[i])
    for i in range(3,jd-1,-1):
        rq.append(str(nf-1)+rqlst[i])
            
    return rq

##########################################################################
#获取营业收入和净利润复合增长率
##########################################################################
def get_g(gpdm_nf_jd_y):
    gpdm=gpdm_nf_jd_y.split('|')[0]
    nf_jd_y=gpdm_nf_jd_y.split('|')[1]
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    gpdm = lgpdm(gpdm)
    sql='select gpdm,rq,yysr,jlr from gpdjb where gpdm=? order by rq desc;'
    curs.execute(sql,(gpdm,))        
    data = curs.fetchall()

    dbcn.close()    

    cols = ['gpdm','rq','yysr','jlr']
    
    df=pd.DataFrame(data,columns=cols)

    nf=int(nf_jd_y.split('_')[0])
    jd=int(nf_jd_y.split('_')[1])
    y =int(nf_jd_y.split('_')[2])

    df1=df[df['rq'].isin(rqttm(nf,jd))]
    df2=df[df['rq'].isin(rqttm(nf-y,jd))]
    
    #检测有没有4个季度的完整数据
    if len(df1)!=4 or len(df2)!=4:
        return [np.nan,np.nan]
    else:        
        s1=df1[['yysr','jlr']].sum()
        s2=df2[['yysr','jlr']].sum()
        #营业收入和净利润为负数时无法计算复合增长率
        if s1['yysr']<=0 :
            s1['yysr']=np.nan
            
        if s1['jlr']<=0 :
            s1['jlr']=np.nan
        
        if s2['yysr']<=0 :
            s2['yysr']=np.nan
            
        if s2['jlr']<=0 :
            s2['jlr']=np.nan
        
        yysr_g=(pow((s1['yysr']/s2['yysr']),1/y)-1)*100
        jlr_g=(pow((s1['jlr']/s2['jlr']),1/y)-1)*100

    return [yysr_g,jlr_g]      


##########################################################################
#获取营业收入TTM
##########################################################################
def get_yysr_ttm(gpdm_nf_jd):
    gpdm=gpdm_nf_jd.split('|')[0]
    nf_jd_y=gpdm_nf_jd.split('|')[1]
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    gpdm = lgpdm(gpdm)
    sql='select gpdm,rq,yysr,jlr from gpdjb where gpdm=? order by rq desc;'
    curs.execute(sql,(gpdm,))        
    data = curs.fetchall()

    cols = ['gpdm','rq','yysr','jlr']
    
    df=pd.DataFrame(data,columns=cols)

    nf=int(nf_jd_y.split('_')[0])
    jd=int(nf_jd_y.split('_')[1])

    df1=df[df['rq'].isin(rqttm(nf,jd))]
    
    #检测有没有4个季度的完整数据
    if len(df1)!=4:
        return pd.Series([np.nan,np.nan],index=['yysr','jlr'])
    else:        
        return df1[['yysr','jlr']].sum()



##########################################################################
#计算营业收入、净利润复合增长率
##########################################################################
def yysr_jlr_g():
    
    jbxx=get_gpdm()
    #2018年1季度3年复合增长率
    nf_jd = '2018_2'
    nf_jd_y='%s_1' % nf_jd 
    zzl0=jbxx['gpdm'].map(lambda x:x+'|'+nf_jd_y).map(get_g)
    zzl0=zzl0.apply(lambda x: pd.Series([x[0],x[1]]))
    zzl0.columns=['yysr_g'+nf_jd_y,'jlr_g'+nf_jd_y]
    jbxx=jbxx.join(zzl0)

    nf_jd_y='%s_2' % nf_jd 
    zzl1=jbxx['gpdm'].map(lambda x:x+'|'+nf_jd_y).map(get_g)
    zzl1=zzl1.apply(lambda x: pd.Series([x[0],x[1]]))
    zzl1.columns=['yysr_g'+nf_jd_y,'jlr_g'+nf_jd_y]
    jbxx=jbxx.join(zzl1)

    nf_jd_y='%s_3' % nf_jd 
    zzl2=jbxx['gpdm'].map(lambda x:x+'|'+nf_jd_y).map(get_g)
    zzl2=zzl2.apply(lambda x: pd.Series([x[0],x[1]]))
    zzl2.columns=['yysr_g'+nf_jd_y,'jlr_g'+nf_jd_y]
    jbxx=jbxx.join(zzl2)

    nf_jd_y='%s_5' % nf_jd 
    zzl3=jbxx['gpdm'].map(lambda x:x+'|'+nf_jd_y).map(get_g)
    zzl3=zzl3.apply(lambda x: pd.Series([x[0],x[1]]))
    zzl3.columns=['yysr_g'+nf_jd_y,'jlr_g'+nf_jd_y]
    jbxx=jbxx.join(zzl3)

    nf_jd_y='%s_7' % nf_jd 
    zzl4=jbxx['gpdm'].map(lambda x:x+'|'+nf_jd_y).map(get_g)
    zzl4=zzl4.apply(lambda x: pd.Series([x[0],x[1]]))
    zzl4.columns=['yysr_g'+nf_jd_y,'jlr_g'+nf_jd_y]
    jbxx=jbxx.join(zzl4)

    #2018年1季度营业收入，净利润TTM计算
    jlr_ttm=jbxx['gpdm'].map(lambda x:x+'|2018_1').map(get_yysr_ttm)
    jlr_ttm=jlr_ttm.to_frame()
    jlr_ttm['yysr_ttm']=[e[0] for e in jlr_ttm['gpdm']]
    jlr_ttm['jlr_ttm']=[e[1] for e in jlr_ttm['gpdm']]
    
    #将单位换算成“亿元”
    jlr_ttm['yysr_ttm']=jlr_ttm['yysr_ttm']/100000000
    jlr_ttm['jlr_ttm']=jlr_ttm['jlr_ttm']/100000000
    
    jlr_ttm=jlr_ttm.drop(columns=['gpdm'])
    jlr_ttm=jlr_ttm.dropna()
    
    jlr_ttm=jlr_ttm.round(4)
    
    df=jbxx.join(jlr_ttm)

    df=df.drop(columns=['gpdm','gpmc','gppy','dm'])
    
    csvfn = os.path.join(samepath(),'srlrzzl.csv')
    df.to_csv(csvfn)
    
    return df


##########################################################################
#计算涨幅与回撤
##########################################################################
def zfhc():
    
    df=get_gpdm()
    zfdf=df['dm'].map(zf_hc_calc)
    
    zfdf=zfdf.to_frame()

    #涨幅
    zfdf['y0_zf']=[e[0] for e in zfdf['dm']]
    zfdf['y1_zf']=[e[1] for e in zfdf['dm']]
    zfdf['y2_zf']=[e[2] for e in zfdf['dm']]
    zfdf['y3_zf']=[e[3] for e in zfdf['dm']]
    zfdf['y5_zf']=[e[4] for e in zfdf['dm']]
    zfdf['m6_zf']=[e[5] for e in zfdf['dm']]
    zfdf['m3_zf']=[e[6] for e in zfdf['dm']]
    zfdf['m2_zf']=[e[7] for e in zfdf['dm']]
    zfdf['m1_zf']=[e[8] for e in zfdf['dm']]
    
    #最大回撤
    zfdf['y0_hc']=[e[9] for e in zfdf['dm']]
    zfdf['y1_hc']=[e[10] for e in zfdf['dm']]
    zfdf['y2_hc']=[e[11] for e in zfdf['dm']]
    zfdf['y3_hc']=[e[12] for e in zfdf['dm']]
    zfdf['y5_hc']=[e[13] for e in zfdf['dm']]
    zfdf['m6_hc']=[e[14] for e in zfdf['dm']]
    zfdf['m3_hc']=[e[15] for e in zfdf['dm']]
    zfdf['m2_hc']=[e[16] for e in zfdf['dm']]
    zfdf['m1_hc']=[e[17] for e in zfdf['dm']]

    #振幅
    zfdf['zf_m9']=[e[18] for e in zfdf['dm']]
    zfdf['zf_m6']=[e[19] for e in zfdf['dm']]
    zfdf['zf_m5']=[e[20] for e in zfdf['dm']]
    zfdf['zf_m4']=[e[21] for e in zfdf['dm']]
    zfdf['zf_m3']=[e[22] for e in zfdf['dm']]
    zfdf['zf_m2']=[e[23] for e in zfdf['dm']]
    zfdf['zf_m1']=[e[24] for e in zfdf['dm']]

    #涨幅均值与标准差
    zfdf['m9_rate_mean']=[e[25] for e in zfdf['dm']]
    zfdf['m9_rate_std'] =[e[26] for e in zfdf['dm']]
    zfdf['m6_rate_mean']=[e[27] for e in zfdf['dm']]
    zfdf['m6_rate_std'] =[e[28] for e in zfdf['dm']]
    zfdf['m5_rate_mean']=[e[29] for e in zfdf['dm']]
    zfdf['m5_rate_std'] =[e[30] for e in zfdf['dm']]
    zfdf['m4_rate_mean']=[e[31] for e in zfdf['dm']]
    zfdf['m4_rate_std'] =[e[32] for e in zfdf['dm']]
    zfdf['m3_rate_mean']=[e[33] for e in zfdf['dm']]
    zfdf['m3_rate_std'] =[e[34] for e in zfdf['dm']]
    zfdf['m2_rate_mean']=[e[35] for e in zfdf['dm']]
    zfdf['m2_rate_std'] =[e[36] for e in zfdf['dm']]
    zfdf['m1_rate_mean']=[e[37] for e in zfdf['dm']]
    zfdf['m1_rate_std'] =[e[38] for e in zfdf['dm']]


    #近期涨跌幅度
    zfdf['d2_zf']=[e[39] for e in zfdf['dm']]
    zfdf['d3_zf']=[e[40] for e in zfdf['dm']]
    zfdf['d4_zf']=[e[41] for e in zfdf['dm']]
    zfdf['d5_zf']=[e[42] for e in zfdf['dm']]
    zfdf['d7_zf']=[e[43] for e in zfdf['dm']]
    zfdf['d10_zf']=[e[44] for e in zfdf['dm']]
    zfdf['d12_zf']=[e[45] for e in zfdf['dm']]
    zfdf['d15_zf']=[e[46] for e in zfdf['dm']]
    zfdf['d20_zf']=[e[47] for e in zfdf['dm']]

    zfdf=zfdf.drop(columns=['dm'])
    zfdf=zfdf.round(2)

    zfdf.to_csv('zfdf.csv')

    return 


##########################################################################
#
##########################################################################
def xg_peg():
    now = datetime.datetime.now().strftime('%Y%m%d_%H%M')

    #获取股票基本信息
    jbxx=get_jbxx()
    
    #2018年1季度3年复合增长率
    srlrzzl=yysr_jlr_g()
    
    df=jbxx.join(srlrzzl)

    df=df.assign(eps_ttm = df['jlr_ttm']/df['zgb'])

    
    #5月1日后用上年年报eps
    df['eps']=df['eps2017']
    df['jlr_g']=df['eps_g2017']
    df['jlr_1']=df['jlr2016']
    
    
    #计算PE静态市盈率
    df=df.assign(pe0 = df['gj']/df['eps'])
    #eps小于等于0,PE无意义
    df.loc[df['eps']<=0,'pe0']=np.nan
    #股价为0,停牌
    df.loc[df['gj']<=0,'pe0']=np.nan
    
    #计算PEG0
    df=df.assign(peg0 = df['pe0']/df['jlr_g'])
 
    #净利润同比增长率为负的，PEG也没有意义
    df.loc[df['jlr_g']<=0,'peg0']=np.nan
    
    #上一年净利润为负，净利润同比增长率也没有意义
    df.loc[df['jlr_1']<0,'peg0']=np.nan

    #计算pb0
    df=df.assign(pb0 = df['gj']/df['mgjzc'])
    df.loc[df['mgjzc']<=0,'pb0']=np.nan
    df.loc[df['gj']<=0,'pb0']=np.nan

    #盈利预测    
    ylyc=ylyc2018()
    
    df=df.join(ylyc)
    
    df=df.assign(eps1_g = (df['mean']/df['eps']-1)*100)
    df=df.assign(eps1_g_x = (df['amax']/df['eps']-1)*100)
    df=df.assign(eps1_g_n = (df['amin']/df['eps']-1)*100)

    #未来一年预测pe1、peg1
    df=df.assign(pe1 = df['gj']/df['mean'])

    df.loc[df['mean']<=0,'pe1']=np.nan
    df.loc[df['gj']<=0,'pe1']=np.nan

    df=df.assign(peg1 = df['pe1']/df['eps1_g'])

    df.loc[df['pe1']<=0,'peg1']=np.nan
    df.loc[df['eps1_g']<=0,'peg1']=np.nan

    df=df.round(2)
    
    zfdf=pd.read_csv('zfdf.csv')
    zfdf=zfdf.set_index('gpdm')

    df=df.join(zfdf)

#    stk=df.copy()
    

    #选股说明
    xgsm={}
    xgsm['选股时间']=now
    #选股

    #上市时间
    df1=df[(df['ssrq']<'20150101')]
    xgsm['上市日期']='2015年1月1日前上市'

    df1=df1[(df1['eps1_g']>=10)]
    xgsm['2018业绩预测']='eps增长率预测>10%'

    #股东户数同比减少
    df1=df1[(df1['gdhstb']<50)]
    xgsm['股东户数同比']='当前股东户数比2016年12月31日增长不超过50%'

    #股东户数季度环比不大于
    df1=df1[~(df1['gdhshb']>50)]
    xgsm['股东户数季度环比减少']='当前股东户数比2017年9月30日的增长不超过50%，也就是没有出现大幅增长。'

    df1=df1[(df1['size']>=2)]
    xgsm['2018年盈利预测机构数']='有2家以上机构给出盈利预测'

    df1=df1[(df1['pe1']<60)]
    xgsm['2018年市盈率预测值']='小于60'

    df1=df1[(df1['peg1']<0.8)]
    xgsm['2018年PEG预测值']='小于0.8'

#    df1=df1[~(df1['roe']<10)]
#    xgsm['2017年ROE']='不小于10，保留ROE为空或大于10的。'
    
    df1=df1[(df1['roe2017']>5)]
    xgsm['2017年ROE']='大于5。'


    df1=df1[(df1['jlr_g_bgq']>df1['eps1_g']*0.9)]
    xgsm['最新业绩同比增长']='大于机构预测。'


    ser=pd.Series(xgsm)
    ser.index.name='项目名称'
    ser.name='项目参数'

    #Python pandas 数据框的str列内置的方法详解
    #https://blog.csdn.net/qq_28219759/article/details/52919233
    df2=df1[~(df1['zyts'].str.contains('解禁'))]
#    df2=df2[~(df2['zyts'].str.contains('减持'))]
#    df2=df2[~(df2['zyts'].str.contains('高质押'))]
    
    df3=df2[(df2['jll2017']>15)]
    df3=df3[(df3['roe2017']>10)]
    df3=df3[(df3['roa2017']>6)]
    df3=df3[(df3['yj'].str.contains('增'))]

    fn = r'd:\selestock\peg2018_'+now+'.xlsx'

    if os.path.exists(fn):
        os.remove(fn)

    jbxx=jbxx.drop(columns=['gpdm','dm'])
    df=df.drop(columns=['gpdm','dm'])
    df1=df1.drop(columns=['gpdm','dm'])
    df2=df2.drop(columns=['gpdm','dm'])
    df3=df3.drop(columns=['gpdm','dm'])

    writer=pd.ExcelWriter(fn,engine='xlsxwriter')

    jbxx.to_excel(writer, sheet_name='基本信息')   
    df.to_excel(writer, sheet_name='业绩信息')   
    df1.to_excel(writer, sheet_name='选股一')  
    ser.to_excel(writer, sheet_name='选股一参数')  
    df2.to_excel(writer, sheet_name='选股二(去掉半年内有解禁)')  
    df3.to_excel(writer, sheet_name='选股三(roa＞6、jll＞15、roe＞10)')  

    writer.save()

##########################################################################
#2017年5月1日股票池
##########################################################################
def pool_peg(nf):
    rq1=str(nf-3)+'0101'
    rq2=str(nf-2)+'.12.31'
    rq3=str(nf-1)+'.12.31'
    ssrq=get_ssrq()
    ssrq=ssrq[ssrq['ssrq']<rq1]
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()

    sql='''select gpdm,eps0,eps0_g from gpeps where rq=? and eps0>=0.05 and eps0_g>15
        and gpdm in (select gpdm from gpeps where rq=? and eps0>=0.05)
    ;'''
    curs.execute(sql,(rq3,rq2))        
    data = curs.fetchall()

    dbcn.close()    

    cols = ['gpdm','eps0','eps0_g']
    
    df=pd.DataFrame(data,columns=cols)
    df=df.set_index('gpdm')
    df=df[df.index.isin(ssrq.index)]
    
    return df

 
##########################################################################
#提取自由现金流含量、资产报酬率、净利率
##########################################################################
def get_zyxjhl_roa_jll(nf1,nf2):
    df1 = get_srzyxjhl1(nf1,nf2)
    df2 = get_srzyxjhl2(nf1,nf2)
    df3 = get_roa(nf1,nf2)
    df4 = get_jll(nf1,nf2)
    df5 = get_roa(nf1,nf2,5)
    
    df=df1.join(df2,how='outer')
    df=df.join(df3,how='outer')
    df=df.join(df4,how='outer')
    df=df.join(df5,how='outer')
    
    return df    

###############################################################################
#股份回购
###############################################################################
def gphg():
    '''
    select * from gphg where ssjd in ('董事会预案','股东大会通过','实施中') and ggrq>='2017-07-01';
    '''
    blk = {}
    td=datetime.datetime.now()
    m1=(td+datetime.timedelta(-365)).strftime("%Y-%m-%d")
    
    dbfn=getdrive()+'\\hyb\\STOCKDATA.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    sql='''
        select distinct gpdm from gphg where ssjd in ('董事会预案','股东大会通过','实施中') 
        and ggrq>='%s';
        ''' % m1
    curs.execute(sql)        
    data = curs.fetchall()

    dbcn.close()    

    stk=[e[0][:6] for e in data]
    stnum=len(stk)
    blkname='股份回购'
    if stnum>0:
        blk[blkname] = [blkname,stnum,stk]

    return blk


    
###############################################################################
#高管股东持股变动信息
#读取d:\hyb\STOCKDATA.db中的gggdcgbd表生成，
#该表由D:/selestock/gaolt_gggdzjc2sqlite.py
#通过读取http://stock.romaway.com/stock/main.asp?StockCode=%s&Flag=4中的港澳资讯 灵通V7.0
#通达信的F10就是调用的该数据
###############################################################################
def gggdcgbdxx():
    blk = {}
    td=datetime.datetime.now()

    m0=td.strftime("%Y-%m-%d")
    m1=(td+datetime.timedelta(-30*1)).strftime("%Y-%m-%d")
    m2=(td+datetime.timedelta(-30*3)).strftime("%Y-%m-%d")
    m3=(td+datetime.timedelta(-30*6)).strftime("%Y-%m-%d")


    jjlst=[['半年增持',m2,m3,'增持'],['三月增持',m1,m2,'增持'],['月内增持',m0,m1,'增持'],
           ['半年减持',m2,m3,'减持'],['三月减持',m1,m2,'减持'],['月内减持',m0,m1,'减持']]   
    dbfn=getdrive()+'\\hyb\\STOCKDATA.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    for blkname,i,j,k in jjlst:
        sql='''select distinct gpdm from gggdcgbd 
               where bdrq<="%s" and bdrq>"%s" and bdfx="%s" 
            ;'''  % (i,j,k)
        curs.execute(sql)        
        data = curs.fetchall()
        stk=[e[0][:6] for e in data]
        stnum=len(stk)
        if stnum>0:
            blk[blkname] = [blkname,stnum,stk]

    dbcn.close()    

    return blk


###############################################################################
#限售解禁信息
#读取d:\hyb\STOCKDATA.db中的xsjj_dfcf、xsjj_dzh、xsjj_ths、xsjj_gao表生成，
#xsjj_dfcf表由D:/selestock/eastmoney_xsjjhz2sqlite.py
#通过读取http://data.eastmoney.com/dxf/detail.aspx?market=all中的东方财富网
#解禁详情一览数据
#    
#xsjj_dzh表由D:/hyb/dzh_xsjjhz2sqlite.py
#通过读取http://webf10.gw.com.cn/'+sc+'/B11/'+sc+gpdm+'_B11.html中的大智慧网
#限售股解禁汇总
#    
###############################################################################
def xsjjxx():
    blk = {}

    y=datetime.date.today().year
    m=datetime.date.today().month    
    m0=datetime.date(y,m,1).strftime("%Y-%m-%d")
    m1=datetime.date(y+(0 if m+1<=12 else 1),(m+1 if m+1<=12 else m+1-12),1).strftime("%Y-%m-%d")
    m2=datetime.date(y+(0 if m+2<=12 else 1),(m+2 if m+2<=12 else m+2-12),1).strftime("%Y-%m-%d")
    m3=datetime.date(y+(0 if m+3<=12 else 1),(m+3 if m+3<=12 else m+3-12),1).strftime("%Y-%m-%d")
    m6=datetime.date(y+(0 if m+6<=12 else 1),(m+6 if m+6<=12 else m+6-12),1).strftime("%Y-%m-%d")

    jjlst=[['本月解禁',m0,m1],['下月解禁',m1,m2],['三月解禁',m2,m3],['半年解禁',m3,m6]]   
    dbfn=getdrive()+'\\hyb\\STOCKDATA.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    for blkname,i,j in jjlst:
        sql='''select distinct gpdm from (
        select distinct gpdm from xsjj_dfcf where jjrq>="%s" and jjrq<"%s" 
        union select distinct gpdm from xsjj_dzh where jjrq>="%s" and jjrq<"%s"
        union select distinct gpdm from xsjj_ths where jjrq>="%s" and jjrq<"%s"
        union select distinct gpdm from xsjj_gao where jjrq>="%s" and jjrq<"%s")
        ;'''  % (i,j,i,j,i,j,i,j)
        curs.execute(sql)        
        data = curs.fetchall()
        stk=[e[0][:6] for e in data]
        stnum=len(stk)
        if stnum>0:
            blk[blkname] = [blkname,stnum,stk]

    dbcn.close()    

    return blk


########################################################################
#获取本机通达信安装目录，生成自定义板块保存目录
#输入参数："gn"，"fg"，"zs"
#输出形如：    
#{'3D打印': ['3D打印',
#  39,
#  ['000928',
#   '000938',
#   '000969',
#   ......
#   '603167']],
# '黄金概念': ['黄金概念',
#  30,
#  ['000587',
#   '000975',
#   '002102',
#   ......
#   '601212',
#   '601899']]}
#   注意：由于每个板块最多400只股票，融资融券有973只    
########################################################################
def gettdxblk(lb):

    blkfn = gettdxdir() + '\\T0002\\hq_cache\\block_'+lb+'.dat'
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
                
            #去掉两个指数板块
            if blkname not in ['重点沪指','精选指数']:
                blk[blkname] = [blkname,stnum,stk]
    
            f.read((400-stnum)*7)
                
        f.close()

    return blk

###############################################################################
#通达信概念、风格、指数
###############################################################################
def tdxgnfgzs():
    gfzdic={}
    gfz=['gn','fg','zs']
    for lb in gfz:
        gfzdic.update(gettdxblk(lb))

    return gfzdic


###############################################################################
#zyts重要提示：所属通达信概念、风格、指数，限售解禁、高管股东持股变动
#提取股票所属通达信概念、风格、指数中重要的概念、风格与指数形成pd
###############################################################################
def get_zyts():
    
    #通达信概念、风格、指数
    gfz=tdxgnfgzs()
    #限售解禁
    xsjj=xsjjxx()
    #高管、股东持股变动
    zjc=gggdcgbdxx()
    
    blkname=['沪深300','银河99','MSCI成份','上证50','中证100','央视50','高质押股','证金持股','养老金',
             '区块链','芯片','独角兽','仿制药','国证成长','国证价值','陆股通买','陆股通卖',
             '保险重仓', 'QFII重仓', '信托重仓', '券商重仓', '社保重仓', '基金重仓', 
             '高管增持', '高管减持','股份回购','本月解禁','下月解禁','三月解禁','半年解禁','行业龙头',
             '半年增持', '半年减持','三月增持', '三月减持','月内增持', '月内减持','破净资产','低市盈率','低市净率']   
    stkdic={}
    for blk in blkname:
        if blk in gfz.keys():
            bkname,bknum,bkstk=gfz[blk]
            for i in range(bknum):
                gpdm=lgpdm(bkstk[i])
                if gpdm in stkdic.keys():
                    stkdic[gpdm]=stkdic[gpdm]+'_'+bkname
                else:
                    stkdic[gpdm]=bkname
                    
            #gfz和xsjj都有“本月解禁”        
        if blk in xsjj.keys():
            bkname,bknum,bkstk=xsjj[blk]
            for i in range(bknum):
                gpdm=lgpdm(bkstk[i])
                if gpdm in stkdic.keys():
                    if bkname not in stkdic[gpdm]:
                        stkdic[gpdm]=stkdic[gpdm]+'_'+bkname
                else:
                    stkdic[gpdm]=bkname
        
        if blk in zjc.keys():
            bkname,bknum,bkstk=zjc[blk]
            for i in range(bknum):
                gpdm=lgpdm(bkstk[i])
                if gpdm in stkdic.keys():
                    if bkname not in stkdic[gpdm]:
                        stkdic[gpdm]=stkdic[gpdm]+'_'+bkname
                else:
                    stkdic[gpdm]=bkname

    df=pd.DataFrame.from_dict(stkdic, orient='index')
    df.index.name='gpdm'
    df.columns=['zyts']            

    return df        
        
    
##########################################################################
#从tushare获取复权因子
##########################################################################
def get_adjfactor(gpdm):
    gpdm=sgpdm(gpdm)
#    url='http://file.tushare.org/tsdata/f/factor/%s.csv' 
    url='d:\\adjf\\%s.csv' % gpdm
    df = pd.read_csv(url)
    df = df.set_index('datetime')

    return df

##########################################################################
#程序路径，工作目录
##########################################################################
def samepath():
    if sys.argv[0]!='' :
        (filepath,tempfilename) = os.path.split(sys.argv[0])
        return filepath
    else :
        return os.getcwd()


##########################################################################
#查看是否需要下载
#如果文件是最后一个交易日16:00以后生成的，则不需要下载
##########################################################################
def dl_yes(csvfn):
    dlyes = False    #下载标志，True表示要下载

    if os.path.exists(csvfn):
        
        mtime=os.path.getmtime(csvfn)  #文件建立时间
        ltime=time.strftime("%Y%m%d",time.localtime(mtime))
        if ltime >= lastopenday() :
            #必须是最后一个交易日23点以后下载的最新数据
            if ltime == lastopenday() and time.strftime("%H",time.localtime(mtime))<'23':
                dlyes = True
            else:    
                dlyes = False
        else :
            dlyes = True
    else :
        dlyes = True

    return dlyes
    
##########################################################################
#查看文件是否是最新交易日指定时间后的，且当前时间是下载时间段，是则不需要下载或生成
#
##########################################################################
def dl_fn(fn,hr):
    dlyes = False    #下载标志，True表示要下载
    now = datetime.datetime.now().strftime('%Y%m%d%H')
    
    ldt=lastopenday()+str(100+hr)[1:]
    
    if os.path.exists(fn):

        mtime=os.path.getmtime(fn)  #文件建立时间
        ltime=time.strftime("%Y%m%d%H",time.localtime(mtime))

        #文件是在最新交易日指定时间前的且当前时间是最新交易日指定下载时间之后
        if ltime < ldt and now>=ldt:
            dlyes = True
        else:    
            dlyes = False
    else :
        dlyes = True
                
    return dlyes
    
##########################################################################
#查看文件创建时间，是否是最新交易日指定时间后的，且当前时间是下载时间段，是则不需要下载或生成
#
##########################################################################
def gen_fn(fn,hr):
    dlyes = False    #下载标志，True表示要下载
    now = datetime.datetime.now().strftime('%Y%m%d%H')
    
    ldt=lastopenday()+str(100+hr)[1:]
    
    if os.path.exists(fn):

        mtime=os.path.getmtime(fn)  #文件建立时间
        ltime=time.strftime("%Y%m%d%H",time.localtime(mtime))

        #文件是在最新交易日指定时间前的且当前时间是最新交易日指定下载时间之后
        if ltime < ldt and now>=ldt:
            dlyes = True
        else:    
            dlyes = False
    else :
        dlyes = True
                
    return dlyes
    

##########################################################################
#最新股价
#更新时间：交易日每天15点～16点之间
#    pro.daily
#    ts_code 	str 	股票代码
#    trade_date 	str 	交易日期
#    open 	float 	开盘价
#    high 	float 	最高价
#    low 	float 	最低价
#    close 	float 	收盘价
#    pre_close 	float 	昨收价
#    change 	float 	涨跌额
#    pct_chg 	float 	涨跌幅 （未复权，如果是复权请用 通用行情接口 ）
#    vol 	float 	成交量 （手） 
#    amount 	float 	成交额 （千元）
##########################################################################
def get_todaygj():
    
    mytoken='18fcea168f6c1f8621c13bef376e726cf5e31fde3f579db37929181b'
    pro = ts.pro_api(token=mytoken)

    df = pro.daily(trade_date=lastopenday(),fields='ts_code,close')
    
    df = df[['ts_code','close']]
    
    df.columns=['gpdm','gj']
    
    df = df.set_index('gpdm')
    
    return df        

##########################################################################
#查看是否需要下载复权因子文件
##########################################################################
def dl_adjf0(csvfn,gpdm):
    dlyes = False    #下载标志，True表示要下载

    if os.path.exists(csvfn):
        
        mtime=os.path.getmtime(csvfn)  #文件建立时间
        ltime=time.strftime("%Y-%m-%d",time.localtime(mtime))
        hour = time.strftime("%H",time.localtime(mtime))

        #如果存在复权因子文件且是2018-07-01以后的，没有最新得除权除息日期，则不需下载
        if ltime >='2019-03-01':
            return False

        
        #如果存在复权因子文件，但文件是在最新得除权除息日期以前下载的，则需下载
        if ltime >= cqrq[gpdm] :
            #必须是最后一个交易日18点以后下载的最新数据
            if ltime == cqrq[gpdm] and hour<'17':
                dlyes = True
            else:    
                dlyes = False
        else :
            dlyes = True
    else :
        dlyes = True

    return dlyes
    
##########################################################################
#查看是否需要下载复权因子文件
##########################################################################
def dl_adjf(csvfn,gpdm):
    dlyes = False    #下载标志，True表示要下载

    td=(datetime.datetime.now()+datetime.timedelta(-30)).strftime("%Y-%m-%d")

    if os.path.exists(csvfn):
        
        mtime=os.path.getmtime(csvfn)  #文件建立时间
        ltime=time.strftime("%Y-%m-%d",time.localtime(mtime))
        hour = time.strftime("%H",time.localtime(mtime))

        #如果存在复权因子文件且是最近1个月内下载的，没有最新得除权除息日期，则不需下载
        if ltime >=td and not lgpdm(gpdm) in cqrq.keys():
            return False

        
        #如果存在复权因子文件，但文件是在最新得除权除息日期以前下载的，则需下载
        if ltime >= cqrq[gpdm] :
            #必须是最后一个交易日18点以后下载的最新数据
            if ltime == cqrq[gpdm] and hour<'18':
                dlyes = True
            else:    
                dlyes = False
        else :
            dlyes = True
    else :
        dlyes = True

    return dlyes
    

###############################################################################
#复权因子是从tushare下载的csv文件
###############################################################################
def factor_adj(gpdm):

    csvfn ='d:\\adjf\\%s.csv' % sgpdm(gpdm)

    
    dlyes = dl_adjf(csvfn,lgpdm(gpdm))
    
    if dlyes:
        try:
            df = pd.read_csv('http://file.tushare.org/tsdata/f/factor/%s.csv' % gpdm)
            df.to_csv(csvfn,index=False)
        except:
            #在没有csv文件是生成空df
            ld=lastopenday()
            ld=ld[:4]+'/'+ld[4:6]+'/'+ld[6:]
            data=[[ld,1.000]]
            df=pd.DataFrame(data,columns=['datetime','adj_factor'])            
    else:
        df = pd.read_csv(csvfn)

    df = df.set_index('datetime')
    df.index.name = 'rq'

    df=df.sort_index(ascending = False)
    a = df.iloc[0][0]

    #前复权
    df=df/a

    return df

###############################################################################
#前复权股价，复权因子是从tushare下载的csv文件
###############################################################################
def get_fqgj(gpdm,cols=None):

    data=tdxday2pd(gpdm)

    cols1=['date','adj_close','adj_rate']
    records=[]    
    #未上市新股
    if data.empty:
        return pd.DataFrame(records,columns=cols1)
    
    
    if len(gpdm)==6 and (gpdm[0] =='6' or gpdm[:2]=='00' or gpdm[:3] in ('002','300')):    
        #计算前复权因子
        adjf=factor_adj(gpdm)
        
        #前复权收盘价
        data=data.join(adjf)
        #补缺，可以解决tushare还没有更新，而当天收盘结束后缺失复权因子导致无法进行后续涨幅计算
        data['adj_factor'].fillna(method='ffill',inplace=True)
        data=data.eval('adj_close = adj_close * adj_factor')

    s=data['adj_close']
    data['adj_rate']=((s-s.shift(1))/s.shift(1))*100
            
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
    
    return fqgj.round(4)


###############################################################################
#查询dataframe,日期升序排列，返回不小于指定日期的series（最后一天可能小于指定日期），
#超出指定日期5天则还回空
###############################################################################
def get_nearest(df,idx):
    idx=pd.to_datetime(idx)
    df=df.sort_index()
    #如果指定日期在df的时间索引中存在，直接返回指定日期df的序列
    if idx in df.index:
        return df.loc[idx]
    else:
        #如果指定日期不在时间索引中，则从小到大查找
        for i in range(len(df.index)):
            if idx>df.index[i]:
                continue        #小于指定日期，继续
            else:
                break           #大于指定日期，停止
        
        #找到的日期与指定日期相差小于5天，则返回该日期df序列，否则返回空序列
        if abs((idx-df.index[i]).days)<5:
            return df.iloc[i]
        else:
            return pd.Series()      #返回空序列
    
###############################################################################
#一次性计算多个区间涨幅,回撤,振幅
###############################################################################
def zf_hc_calc(gpdm):
    print(gpdm)
    td=datetime.datetime.now()
    nw=td.strftime("%Y%m%d")
    
    y0=str(td.year)+'0101'
    y1=(td+datetime.timedelta(-365)).strftime("%Y%m%d")
    y2=(td+datetime.timedelta(-365*2)).strftime("%Y%m%d")
    y3=(td+datetime.timedelta(-365*3)).strftime("%Y%m%d")
    y5=(td+datetime.timedelta(-365*5)).strftime("%Y%m%d")
    m9=(td+datetime.timedelta(-30*9)).strftime("%Y%m%d")
    m6=(td+datetime.timedelta(-30*6)).strftime("%Y%m%d")
    m5=(td+datetime.timedelta(-30*5)).strftime("%Y%m%d")
    m4=(td+datetime.timedelta(-30*4)).strftime("%Y%m%d")
    m3=(td+datetime.timedelta(-30*3)).strftime("%Y%m%d")
    m2=(td+datetime.timedelta(-30*2)).strftime("%Y%m%d")
    m1=(td+datetime.timedelta(-30*1)).strftime("%Y%m%d")

    df=get_fqgj(gpdm)
    
    #未上市新股返回df为空
    if df.empty:
        return 48*[None]
    
    #最新股价
    gj0=get_nearest(df,nw)
    
    #返回空序列，说明现在停牌，停牌无法计算涨幅，返回None
    zf=[]
    if not gj0.empty:
        
        gj0=gj0['adj_close']

        for dt in (y0,y1,y2,y3,y5,m6,m3,m2,m1):
            gj=get_nearest(df,dt)
            if not gj.empty:
                gj=get_nearest(df,dt)['adj_close']
                zf.append((gj0/gj-1)*100)
            else:
                zf.append(None)

    else:
        zf = 9*[None]
        
    #计算回撤
    for dt in (y0,y1,y2,y3,y5,m6,m3,m2,m1):
        hc=max_drawdown(df.loc[dt:nw,:])['adj_close']
        zf.append(hc)

    #计算振幅
    
    gj0=get_nearest(df,nw)
    #返回空序列，说明现在停牌，停牌无法计算振幅，返回None
    if not gj0.empty:
        
        dt2=nw
        for dt1 in (m9,m6,m5,m4,m3,m2,m1):
            zf.append(calc_zf(df,dt1,dt2))

    else:
        zf.extend(7*[None])
        
    gj0=get_nearest(df,nw)

    #返回空序列，说明现在停牌，停牌无法计算涨幅均值和标准差，返回None
    if not gj0.empty:
        
        dt2=nw
        for dt1 in (m9,m6,m5,m4,m3,m2,m1):
            md=calc_rate_mean_std(df,dt1,dt2)
            zf.append(md[0])
            zf.append(md[1])

    else:
        zf.extend(14*[None])

    #计算涨跌幅度
    rn=zf_n(df)
    zf.extend(rn)
    
    return zf


###############################################################################
#指定日期前2、3、4、5、10、15,20个交易涨跌幅度
###############################################################################
def zf_n(df,dt=None):
    #指定日期可能不是交易日，先
    ds=[2,3,4,5,7,10,12,15,20]
    if dt==None:
        dt=lastopenday()
        
    d0=get_openday_pre(dt,0)

    p=df['adj_close']
    if d0 in p.index:
        p0=p[d0]
    else:
        return len(ds)*[None]

    r = []
    for d in ds:
        dd=get_openday_pre(d0,d)
        if dd in p.index:
            re=round((p0/p[dd]-1)*100.00,2)
        else:
            re=None
            
        r.append(re)

    return r

###############################################################################
#计算区间涨幅均值和标准差
###############################################################################
def calc_rate_mean_std(df,dt1,dt2):
    dt1=dateutil.parser.parse(dt1).strftime("%Y-%m-%d")
    dt2=dateutil.parser.parse(dt2).strftime("%Y-%m-%d")

    '''注意：包含dt1和dt2'''
    df=df[dt1:dt2] 
    m=round(df.adj_rate.mean(),2)
    s=round(df.adj_rate.std(),2)
    return (m,s)
    

###############################################################################
#计算区间振幅
###############################################################################
def calc_zf(df,dt1,dt2):
    dt1=dateutil.parser.parse(dt1).strftime("%Y-%m-%d")
    dt2=dateutil.parser.parse(dt2).strftime("%Y-%m-%d")

    '''注意：包含dt1和dt2'''
    df=df[dt1:dt2] 
    
    return round((df['adj_close'].max()/df['adj_close'].min()-1)*100.00,2)
    

###############################################################################
#一次性计算多个区间振幅
###############################################################################
def zf_calc(gpdm,td=None):
    print(gpdm)
    if not td is None:
        td=dateutil.parser.parse(td)
    else:
        td=datetime.datetime.now()

    nw=td.strftime("%Y%m%d")
    
    m9=(td+datetime.timedelta(-30*9)).strftime("%Y%m%d")
    m6=(td+datetime.timedelta(-30*6)).strftime("%Y%m%d")
    m3=(td+datetime.timedelta(-30*3)).strftime("%Y%m%d")
    m2=(td+datetime.timedelta(-30*2)).strftime("%Y%m%d")
    m1=(td+datetime.timedelta(-30*1)).strftime("%Y%m%d")

    df=get_fqgj(gpdm)
    
    #未上市新股返回df为空
    if df.empty:
        return [None,None,None,None,None]
    
    #最新股价
    gj0=get_nearest(df,nw)
    
    #返回空序列，说明现在停牌，停牌无法计算振幅，返回None
    if not gj0.empty:
        
        zf=[]
        dt2=nw
        for dt1 in (m9,m6,m3,m2,m1):
            zf.append(calc_zf(df,dt1,dt2))

    else:
        zf = [None,None,None,None,None]
        
    return zf

###############################################################################
#最大回撤
###############################################################################
def max_drawdown(df):
    return ((df/df.expanding(min_periods=1).max()).min()-1)*100


###############################################################################
#最新业绩分红配送日期
###############################################################################
def get_yjfprq():
    zxfprq={}
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    
#    sql = 'select gpdm,pe_lyr,pe_ttm,pb from pe_pb where rq="'+date+'";'
#    curs.execute(sql)
    
    curs.execute('select gpdm,cqcxrq from yjfp')
    data = curs.fetchall()

    for gpdm,rq in data:
#        rq=time.mktime(time.strptime(rq, "%Y-%m-%d"))
        zxfprq[gpdm]=rq

    dbcn.close()    
    return zxfprq

    
###############################################################################
#中报选股
###############################################################################
def xg2(df):

#    df=df[(df['ssrq']<'20150701')]
    df=df[(df['gdhsjb']<5)]
    df=df[(df['jlr_g_bgq']>-10)]
#    df=df[(df['gdhsjb']<0)]
    df=df[(df['pe_ttm']<100)]
    df=df[(df['roe2017']>10)]
    df=df[(df['jll2017']>10)]
#    df=df[(df['roa2017']>5)]
    df=df[(df['hjsz']>15)]
    df=df[(df['fzl_2018.03.31']<30)]

    df=df[(df['zyts'].str.contains('增持'))]
    df=df[~(df['zyts'].str.contains('减持'))]

    return df

###############################################################################
#中报选股
###############################################################################
def xg3(df):

#    df=df[(df['ssrq']<'20150701')]
    df=df[(df['gdhsjb']<5)]
    df=df[(df['gdhshb']<-10)]
    df=df[(df['jlr_g_bgq']>10)]
#    df=df[(df['gdhsjb']<0)]
    df=df[(df['pe_ttm']<100)]
#    df=df[(df['roe2017']>10)]
#    df=df[(df['jll2017']>10)]
#    df=df[(df['roa2017']>5)]
    df=df[(df['hjsz']>5)]
    df=df[(df['fzl_2018.06.30']<30)]
    
    df=df[(df['yysr_g2018_2_1']>20)]
    df=df[(df['jlr_g2018_2_1']>20)]
    df=df[(df['y1_zf']>10)]

#    df=df[(df['zyts'].str.contains('增持'))]
#    df=df[~(df['zyts'].str.contains('减持'))]

    return df

###############################################################################
#中报选股
###############################################################################
def xg4(df):

#    df=df[(df['ssrq']<'20150701')]
    df=df[(df['gdhsjb']<5)]
    df=df[(df['gdhshb']<-10)]
    df=df[(df['jlr_g_bgq']>10)]
#    df=df[(df['gdhsjb']<0)]
#    df=df[(df['pe_ttm']<100)]
#    df=df[(df['roe2017']>10)]
#    df=df[(df['jll2017']>10)]
#    df=df[(df['roa2017']>5)]
#    df=df[(df['hjsz']>5)]
#    df=df[(df['fzl_2018.06.30']<30)]
    df=df[(df['yysr2017']>df['yysr2016'])]
    df=df[(df['yysr2016']>df['yysr2015'])]
    df=df[(df['yysr2015']>df['yysr2014'])]
    df=df[(df['yysr2014']>df['yysr2013'])]
    
    df=df[(df['yysr_g2018_2_1']>20)]
    df=df[(df['yysr_g2018_2_2']>20)]
    df=df[(df['yysr_g2018_2_3']>20)]
    df=df[(df['yysr_g2018_2_5']>20)]
    df=df[(df['yysr_g2018_2_7']>20)]
#    df=df[(df['jlr_g2018_2_1']>20)]
#    df=df[(df['y1_zf']>10)]

    df=df[(df['zyts'].str.contains('增持'))]
#    df=df[~(df['zyts'].str.contains('减持'))]

    return df

###############################################################################
#大幅解禁股票代码表
###############################################################################
def dfjj():
    dbfn=getdrive()+'\\hyb\\STOCKDATA.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    sql='''
        select gpdm,jjrq,qltbl from xsjj_dfcf where jjrq>'2018-03-31' and jjrq<='2018-06-30'
        order by qltbl desc;        
        '''
    curs.execute(sql)        
    data = curs.fetchall()
    cols=['gpdm','jjrq','qltbl']
    df=pd.DataFrame(data,columns=cols)

    df=df.drop_duplicates(['gpdm'],keep='first')
    
    df=df.set_index('gpdm')

    dbcn.close()    
    
    return df

###############################################################################
#送转股比例
###############################################################################
def get_szgbl():
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()

    sql = '''select gpdm,szgbl from yjfp where cqcxrq>='2017-12-31'
            ;'''
    
    curs.execute(sql)
    
    data = curs.fetchall()
    cols = ['gpdm','szgbl']
    
    df = pd.DataFrame(data,columns=cols)
    df = df.set_index('gpdm')

    dbcn.close()    

    return df

###############################################################################
#高管股东增减持统计
###############################################################################
def gggdzjctj():
    td=datetime.datetime.now()

    m0=td.strftime("%Y-%m-%d")
    m1=(td+datetime.timedelta(-30*6)).strftime("%Y-%m-%d")

    dbfn=getdrive()+'\\hyb\\STOCKDATA.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    sql='''select distinct gpdm from gggdcgbd 
               where bdrq>="%s" and bdrq<"%s" 
            ;'''  % (m1,m0)
    curs.execute(sql)        
    data = curs.fetchall()
    dbcn.close()
    
    cols=['gpdm']
    df = pd.DataFrame(data,columns=cols)
    df = df.set_index('gpdm',drop=False)
    zjchz=df['gpdm'].map(gggdzjc_sum)

    zjcdf=zjchz.to_frame()

    #高管股东增减持金额汇总，最近六个月、三个月、一个月
    zjcdf['m6zjc']=[e[0] for e in zjcdf['gpdm']]
    zjcdf['m3zjc']=[e[1] for e in zjcdf['gpdm']]
    zjcdf['m1zjc']=[e[2] for e in zjcdf['gpdm']]

    zjcdf=zjcdf.drop(columns=['gpdm'])
    zjcdf=zjcdf.round(2)

    #转换NaN
    zjcdf=zjcdf.replace(0,np.nan) 

    zjcdf.to_csv('zjcdf.csv')
    
    return        
    
###############################################################################
#高管股东增减持统计汇总
###############################################################################
def gggdzjc_sum(gpdm):
    print(gpdm)
    td=datetime.datetime.now()

    m0=td.strftime("%Y-%m-%d")
    m1=(td+datetime.timedelta(-30*6)).strftime("%Y-%m-%d")
    m2=(td+datetime.timedelta(-30*3)).strftime("%Y-%m-%d")
    m3=(td+datetime.timedelta(-30*1)).strftime("%Y-%m-%d")

    dbfn=getdrive()+'\\hyb\\STOCKDATA.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    sql='''select bdrq,bdsl,bdfx from gggdcgbd 
               where gpdm="%s" and bdrq>="%s" and bdrq<"%s" 
            ;'''  % (lgpdm(gpdm),m1,m0)
    curs.execute(sql)        
    data = curs.fetchall()
    cols=['date','bdsl','bdfx']
    df = pd.DataFrame(data,columns=cols)
    df=df.set_index('date')    
    
    gjdf=get_fqgj(gpdm,'close')
    df=df.join(gjdf)    
    #计算变动金额，单位“万元”
    df=df.assign(bdje=df['bdsl']*df['close']/10000)
    m6sum=df.loc[m1:m0,'bdje'].sum()
    m3sum=df.loc[m2:m0,'bdje'].sum()
    m1sum=df.loc[m3:m0,'bdje'].sum()

    return [m6sum,m3sum,m1sum]

'''
Python之系统交互（subprocess）
https://www.cnblogs.com/yyds/p/7288916.html
''' 

###############################################################################
#下载市盈率
###############################################################################
def dl_syl():
    print('正在下载市盈率数据…………')
    fn=syl_pe_fn('syldir')
    lstop=lastopenday()
    if re.findall('csi(\d{8})\.xls',fn)[0]<lstop :
        cmd_re = subprocess.run("pythonw.exe d:\\selestock\\dlsyl.py", shell=True, stdout=subprocess.PIPE)
        print(cmd_re.stdout.decode('GBK'))
        cmd_re = subprocess.run("pythonw.exe d:\\hyb\\syl2sqlite.py", shell=True, stdout=subprocess.PIPE)
        print(cmd_re.stdout.decode('GBK'))
    
    return
    

###############################################################################
#下载市盈率
###############################################################################
def dl_zyl_ltg():
    
    print('正在下载质押率、流通盘数据、除权除息日等数据…………')
    dlfn = {'zyl':[r'd:\selestock\zyl.xls',18],
            'abltg':[r'd:\selestock\ABlt.xls',18],
            'hltg':[r'd:\selestock\Hlt.xls',18],
            'cqcx':[r'd:\selestock\cqcx.xls',18]
            }
    dlfnlst = []
    for k in dlfn:
        #检查是否需要下载
        if dl_fn(dlfn[k][0],dlfn[k][1]):
            dlfnlst.append(k)
            
    if len(dlfnlst):
        dl_ths_xls(dlfnlst)
        
    return

###############################################################################
#下载高管股东增减持数据
###############################################################################
def dl_gggdzjc():
    dlfn1=dl_fn(r'd:\selestock\zjcdf.csv',19)
    if dlfn1:
        print('正在下载高管股东增减持数据，时间较长，请耐心等待……')
        cmd_re = subprocess.run("pythonw.exe d:\\selestock\\dzh_gggdzjc2sqlite.py", shell=True, stdout=subprocess.PIPE)
        if cmd_re.returncode==0:
            print('高管股东增减持数据下载成功！')
        else:
            print('高管股东增减持数据下载失败！')

    return

###############################################################################
#下载研报数据
###############################################################################
def dl_report():
    dlfn1=dl_fn(r'd:\selestock\ylyc.csv',20)
    if dlfn1:
        print('正在从中证网下载研报数据，时间较长，请耐心等待……')
        cmd_re = subprocess.run("pythonw.exe d:\\selestock\\cfi_report2sqlite.py", shell=True, stdout=subprocess.PIPE)
        if cmd_re.returncode==0:
            print('从中证网下载研报数据下载成功！')
        else:
            print('从中证网下载研报数据下载失败！')

        print('正在从大智慧网下载研报数据，时间较长，请耐心等待……')
        cmd_re = subprocess.run("pythonw.exe d:\\selestock\\dzh_report2sqlite.py", shell=True, stdout=subprocess.PIPE)
        if cmd_re.returncode==0:
            print('从大智慧网下载研报数据下载成功！')
        else:
            print('从大智慧网下载研报数据下载失败！')

    return


###############################################################################
#执行选股条件
###############################################################################
def xgtj(df,tjarr):

    for tj in tjarr:
        fld,op,val,typ,com=tj

        #字段与值比较        
        if typ==0 :
            if op =='>' :
                df = df[(df[fld]>val)]             
            elif op == '>=' :
                df = df[(df[fld]>=val)]                         
            elif op == '==' :
                df = df[(df[fld]==val)]                                 
            elif op == '!=' :
                df = df[(df[fld]!=val)]                                             
            elif op == '<' :
                df = df[(df[fld]<val)]                                             
            elif op == '<=' :
                df = df[(df[fld]<=val)]                                             
            elif op == '包含' :
                df=df[(df[fld].str.contains(val))]
            elif op == '不包含' :
                df=df[~(df[fld].str.contains(val))]
                
        #字段与字段比较        
        if typ==1 :
            if op =='>' :
                df = df[(df[fld]>df[val])]             
            elif op == '>=' :
                df = df[(df[fld]>=df[val])]                         
            elif op == '==' :
                df = df[(df[fld]==df[val])]                                 
            elif op == '!=' :
                df = df[(df[fld]!=df[val])]                                             
            elif op == '<' :
                df = df[(df[fld]<df[val])]                                             
            elif op == '<=' :
                df = df[(df[fld]<=df[val])]                                             

    return df        

###############################################################################
#数据准备
###############################################################################
def get_dfdata():
    print('生成数据')
    jbxx=get_jbxx()
    
    #2018年1季度3年复合增长率
    csvfn = os.path.join(samepath(),'srlrzzl.csv')

    if os.path.exists(csvfn):
        srlrzzl=pd.read_csv(csvfn)
        srlrzzl=srlrzzl.set_index('gpdm')
    else:
        srlrzzl=yysr_jlr_g()
    
    df=jbxx.join(srlrzzl)

    df=df.assign(eps_ttm = df['jlr_ttm']/df['zgb'])
    
    df=df.assign(eps_ttm = df['jlr_ttm']/df['zgb'])

    
    #5月1日后用上年年报eps
    #对上年eps送转拆分
    df=df.assign(eps = df['eps2017']/df['szgbl'])
    
    df['jlr_g']=df['eps_g2017']
    
    df['jlr_1']=df['jlr2016']
    
    
    #计算PE静态市盈率
    df=df.assign(pe0 = df['gj']/df['eps'])
    #eps小于等于0,PE无意义
    df.loc[df['eps']<=0,'pe0']=np.nan
    #股价为0,停牌
    df.loc[df['gj']<=0,'pe0']=np.nan
    
    #计算PEG0
    df=df.assign(peg0 = df['pe0']/df['jlr_g'])
 
    #净利润同比增长率为负的，PEG也没有意义
    df.loc[df['jlr_g']<=0,'peg0']=np.nan
    
    #上一年净利润为负，净利润同比增长率也没有意义
    df.loc[df['jlr_1']<0,'peg0']=np.nan

    #计算pb0
    df=df.assign(pb0 = df['gj']/df['mgjzc'])
    df.loc[df['mgjzc']<=0,'pb0']=np.nan
    df.loc[df['gj']<=0,'pb0']=np.nan

    #半年内高管大股东增减持汇总
    zjcdf=pd.read_csv('zjcdf.csv')
    zjcdf=zjcdf.set_index('gpdm')
    df=df.join(zjcdf)

    #半年内高管大股东增持汇总
    zcdf=pd.read_csv('zcdf.csv')
    zcdf=zcdf.set_index('gpdm')
    df=df.join(zcdf)

    #盈利预测    
    ylyc=ylyc2018()
    
    df=df.join(ylyc)
    
    df=df.assign(eps1_g = (df['mean']/df['eps']-1)*100)
    df=df.assign(eps1_g_x = (df['amax']/df['eps']-1)*100)
    df=df.assign(eps1_g_n = (df['amin']/df['eps']-1)*100)

    #未来一年预测pe1、peg1
    df=df.assign(pe1 = df['gj']/df['mean'])

    df.loc[df['mean']<=0,'pe1']=np.nan
    df.loc[df['gj']<=0,'pe1']=np.nan

    df=df.assign(peg1 = df['pe1']/df['eps1_g'])

    df.loc[df['pe1']<=0,'peg1']=np.nan
    df.loc[df['eps1_g']<=0,'peg1']=np.nan

    df=df.round(2)

    zfdf=pd.read_csv('zfdf.csv')
    zfdf=zfdf.set_index('gpdm')

    df=df.join(zfdf)

    df=df.drop(columns=['gpdm','dm'])

    return df    

###############################################################################
#抓取股东户数
###############################################################################
def dl_gdhs():

    print('正在下载股东户数数据…………')
    
    cmd_re = subprocess.run("pythonw.exe D:\\hyb\\eastmoney_gdhs2sqlite.py", shell=True, stdout=subprocess.PIPE)
    print(cmd_re.stdout.decode('GBK'))
    
    return
    
    
if __name__ == '__main__':
#def sele_stk():
    
    #读取最新中证行业和PE、PB信息
#    gpdmb=get_gpdm()
#    gppepb=get_pepb_zzhy()
#    gpgj=get_todaygj()
#    df=gpdmb.join(gppepb)
#    df=df.join(gpgj)
#    df=df.assign(eps_ttm=df['gj']/df['pe_ttm'])
#    df=df.assign(mgjzc=df['gj']/df['pb'])
#    
#    zyts=get_zyts()
#    
#    sys.exit()
    print('%s Running' % sys.argv[0])
    now0 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now0)

#    zyts=get_zyts()

    now1 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now0)
    print('开始运行时间：%s' % now1)
    
    #计算标志，在dlday()设置
    calc_zfdf=False

    #下载开始时间
    start_time = time.time()
    dlday()

#    
    now1 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now0)
    print('开始运行时间：%s' % now1)
#    sys.exit()
    #下载最新市盈率
    dl_syl()

    #下载最新质押率、流通股信息、最新除权除息日
    dl_zyl_ltg()

    #最新除权除息日期
    cqrq=get_cqcxrq()
    #开市日期列表
    odays=openday_list()

    #注意：要先查看最新除权除息日期
#    calc_zfdf=True
    if calc_zfdf:
        zfhc()

    #下载最新高管股东增减持数据
#    dl_gggdzjc()
    
    #从东方财富网抓取最新股东人数
#    dl_gdhs()
#    yysr_jlr_g()
#    sys.exit()

#    dqgj=get_todaygj()


    now2 = datetime.datetime.now().strftime('%H:%M:%S')
    print('下载完成时间：%s' % now2)

    #获取股票基本信息
    df=get_dfdata()

    now = datetime.datetime.now().strftime('%Y%m%d_%H%M')

    fn = r'd:\selestock\peg2018_'+now+'.xlsx'

    if os.path.exists(fn):
        os.remove(fn)


    writer=pd.ExcelWriter(fn,engine='xlsxwriter')

    df.to_excel(writer, sheet_name='基本信息')   
    tjsm=['字段','运算符','值','值类型','含义']


    tjarr=[
        ['ssrq','<','20150701',0,'上市日期早于2015年7月1日'],
#        ['eps1_g','>=',10,0,'2018业绩预测eps增长率预测>10%'],
#        ['gdhstb','<',-5,0,'股东户数比上年同期增长小于-5%'],
#        ['gdhshb','<',-5,0,'股东户数比上季度增长小于-5%'],
#        ['pe_ttm','<',20,0,'滚动市盈率小于20'],
#        ['size','>=',2,0,'2018年盈利预测机构数有2家以上机构给出盈利预测'],
#        ['pe1','<',60,0,'2018年市盈率预测值小于30'],
#        ['peg1','<',0.8,0,'2018年PEG预测值小于0.8'],
#        ['roe2017','>',10,0,'2017年ROE大于10%'],
#        ['jlr_g_bgq','>','eps1_g',1,'最新报告期净利润同比增长大于机构预测'],
        ['gpmc','包含','ST',0,'ST板块'],
#        ['zyts','不包含','解禁',0,'重要提示中不包含【解禁】'],
#        ['jlr_g_bgq','>',10,0,'最新报告期净利润同比增长大于10%'],
        ]

    stk=df.copy()
    stk=xgtj(stk,tjarr)
#    tjdf=pd.DataFrame(tjarr,columns=tjsm)
    stk.to_excel(writer, sheet_name='ST板块')  
#    tjdf.to_excel(writer, sheet_name='低市盈率选股',index=False)  

    tjarr=[
        ['ssrq','<','20150701',0,'上市日期早于2015年7月1日'],
#        ['eps1_g','>=',10,0,'2018业绩预测eps增长率预测>10%'],
        ['gdhstb','<',-5,0,'股东户数比上年同期增长小于-5%'],
        ['gdhshb','<',-5,0,'股东户数比上季度增长小于-5%'],
        ['pe_ttm','<',20,0,'滚动市盈率小于20'],
#        ['size','>=',2,0,'2018年盈利预测机构数有2家以上机构给出盈利预测'],
#        ['pe1','<',60,0,'2018年市盈率预测值小于30'],
#        ['peg1','<',0.8,0,'2018年PEG预测值小于0.8'],
        ['roe2017','>',10,0,'2017年ROE大于10%'],
#        ['jlr_g_bgq','>','eps1_g',1,'最新报告期净利润同比增长大于机构预测'],
        ['zyts','不包含','减持',0,'重要提示中不包含【减持】'],
        ['zyts','不包含','解禁',0,'重要提示中不包含【解禁】'],
#        ['jlr_g_bgq','>',10,0,'最新报告期净利润同比增长大于10%'],
        ]

    stk=df.copy()
    stk=xgtj(stk,tjarr)
    tjdf=pd.DataFrame(tjarr,columns=tjsm)
    stk.to_excel(writer, sheet_name='低市盈率')  
    tjdf.to_excel(writer, sheet_name='低市盈率选股',index=False)  



    stk1=df.copy()
    tjarr1=[
        ['ssrq','<','20150701',0,'上市日期早于2015年7月1日'],
        ['eps1_g','>=',10,0,'2018业绩预测eps增长率预测>10%'],
        ['gdhstb','<',10,0,'股东户数比上年同期增长小于10%'],
        ['gdhshb','<',10,0,'股东户数比上季度增长小于10%'],
        ['size','>=',2,0,'2018年盈利预测机构数有2家以上机构给出盈利预测'],
        ['pe1','<',60,0,'2018年市盈率预测值小于30'],
        ['peg1','<',0.8,0,'2018年PEG预测值小于0.8'],
        ['roe2017','>',5,0,'2017年ROE大于5%'],
        ['jlr_g_bgq','>','eps1_g',1,'最新报告期净利润同比增长大于机构预测'],
        ['zyts','不包含','减持',0,'重要提示中不包含【减持】'],
        ['zyts','不包含','解禁',0,'重要提示中不包含【解禁】'],
        ['jlr_g_bgq','>',10,0,'最新报告期净利润同比增长大于10%'],
        ]

    stk1=xgtj(stk1,tjarr1)
    tjdf1=pd.DataFrame(tjarr1,columns=tjsm)
    stk1.to_excel(writer, sheet_name='选股一')  
    tjdf1.to_excel(writer, sheet_name='选股一参数',index=False)  


    stk2=df.copy()

    tjarr2=[
        ['ssrq','<','20150701',0,'上市日期早于2015年7月1日'],
        ['gdhsjb','<',5,0,'股东户数比上次不超过5%'],
        ['gdhshb','<',-10,0,'股东户数比上季度减少-10%以上'],
        ['jlr_g_bgq','>',10,0,'最新报告期净利润同比增长大于10%'],
        ['zyts','包含','增持',0,'重要提示中包含【增持】'],
        ['zyts','不包含','减持',0,'重要提示中不包含【减持】'],
        ['pe_ttm','<',100,0,'滚动市盈率小于100'],
        ['roe2017','>',10,0,'2017年ROE大于10%'],
        ['jll2017','>',10,0,'2017年净利率大于10%'],
        ['hjsz','>',15,0,'户均市值大于15万元'],
        ['fzl_2018.06.30','<',30,0,'2018年中报负债率小于30%'],
        ['yysr_g2018_2_1','>',20,0,'营业收入2018年中报一年增长率大于20%'],
        ['jlr_g2018_2_1','>',20,0,'净利润2018年中报一年增长率大于20%'],
        ['y1_zf','>',10,0,'股价1年涨幅大于10%'],
        ]

    stk2=xgtj(stk2,tjarr2)
    tjdf2=pd.DataFrame(tjarr2,columns=tjsm)
    stk2.to_excel(writer, sheet_name='选股二')  
    tjdf2.to_excel(writer, sheet_name='选股二参数',index=False)  

    stk3=df.copy()
    tjarr3=[
        ['jlr_g_bgq','>',15,0,'最新报告期净利润同比增长大于15%'],
        ['pe_ttm','<',100,0,'滚动市盈率小于100'],
        ['hjsz','>',15,0,'户均市值大于15万元'],
        ['fzl_2018.06.30','<',30,0,'2018年中报负债率小于30%'],
        ['zyts','包含','增持',0,'重要提示中包含【增持】'],
        ]
    
    stk3=xgtj(stk3,tjarr3)
    tjdf3=pd.DataFrame(tjarr3,columns=tjsm)
    stk3.to_excel(writer, sheet_name='选股三')  
    tjdf3.to_excel(writer, sheet_name='选股三参数',index=False)  


    stk4=df.copy()
    tjarr4=[
        ['yysr2017','>','yysr2016',1,'2017年营业收入大于2016年营业收入'],
        ['yysr2016','>','yysr2015',1,'2016年营业收入大于2015年营业收入'],
#        ['yysr2015','>','yysr2014',1,'2015年营业收入大于2014年营业收入'],
#        ['yysr2014','>','yysr2013',1,'2014年营业收入大于2013年营业收入'],
        ['jlr2017','>','jlr2016',1,'2017年净利润大于2016年净利润'],
        ['jlr2016','>','jlr2015',1,'2016年净利润大于2015年净利润'],
#        ['jlr2015','>','jlr2014',1,'2015年净利润大于2014年净利润'],
#        ['jlr2014','>','jlr2013',1,'2014年净利润大于2013年净利润'],
        ['gdhsjb','<',5,0,'股东户数比上次不超过5%'],
        ['gdhshb','<',-10,0,'股东户数比上季度减少-10%以上'],
        ['jlr_g_bgq','>',10,0,'最新报告期净利润同比增长大于10%'],
        ['jlr','>',0,0,'最新报告期净利润大于0'],
#        ['zyts','包含','增持',0,'重要提示中包含【增持】'],
        ['zyts','不包含','减持',0,'重要提示中不包含【减持】'],
        ['zyts','不包含','高质押',0,'重要提示中不包含【高质押】'],
        ['zyts','不包含','解禁',0,'重要提示中不包含【解禁】'],

        ]
    
    stk4=xgtj(stk4,tjarr4)
    tjdf4=pd.DataFrame(tjarr4,columns=tjsm)
    stk4.to_excel(writer, sheet_name='选股四')  
    tjdf4.to_excel(writer, sheet_name='选股四参数',index=False)  


    stk5=df.copy()
    tjarr5=[
        ['yysr2017','>','yysr2016',1,'2017年营业收入大于2016年营业收入'],
        ['yysr2016','>','yysr2015',1,'2016年营业收入大于2015年营业收入'],
#        ['yysr2015','>','yysr2014',1,'2015年营业收入大于2014年营业收入'],
#        ['yysr2014','>','yysr2013',1,'2014年营业收入大于2013年营业收入'],
        ['jlr2017','>','jlr2016',1,'2017年净利润大于2016年净利润'],
        ['jlr2016','>','jlr2015',1,'2016年净利润大于2015年净利润'],
#        ['jlr2015','>','jlr2014',1,'2015年净利润大于2014年净利润'],
#        ['jlr2014','>','jlr2013',1,'2014年净利润大于2013年净利润'],
        ['gdhsjb','<',5,0,'股东户数比上次不超过5%'],
        ['gdhshb','<',-10,0,'股东户数比上季度减少-10%以上'],
        ['jlr_g_bgq','>',10,0,'最新报告期净利润同比增长大于10%'],
        ['jlr','>',0,0,'最新报告期净利润大于0'],
        ['zyts','包含','增持',0,'重要提示中包含【增持】'],
        ['zyts','不包含','减持',0,'重要提示中不包含【减持】'],
        ['zyts','不包含','高质押',0,'重要提示中不包含【高质押】'],
        ['zyts','不包含','解禁',0,'重要提示中不包含【解禁】'],
        ['fzl_2018.06.30','<',30,0,'2018年中报负债率小于30%'],
        ['hjsz','>',15,0,'户均市值大于15万元'],

        ]
    
    stk5=xgtj(stk5,tjarr5)
    tjdf5=pd.DataFrame(tjarr5,columns=tjsm)
    stk5.to_excel(writer, sheet_name='选股五')  
    tjdf5.to_excel(writer, sheet_name='选股五参数',index=False)  


    stk6=df.copy()

    tjarr6=[
        ['ssrq','<','20170701',0,'上市日期早于2015年7月1日'],
        ['gdhsjb','<',5,0,'股东户数比上次不超过5%'],
        ['gdhshb','<',10,0,'股东户数比上季度增长小于10%'],
        ['jlr_g_bgq','>',5,0,'最新报告期净利润同比增长大于5%'],
#        ['zyts','包含','增持',0,'重要提示中包含【增持】'],
#        ['zyts','不包含','减持',0,'重要提示中不包含【减持】'],
        ['pe_ttm','<',30,0,'滚动市盈率小于30'],
        ['roe2017','>',10,0,'2017年ROE大于15%'],
        ['jll2017','>',15,0,'2017年净利率大于15%'],
        ['hjsz','>',10,0,'户均市值大于10万元'],
        ['fzl_2018.06.30','<',30,0,'2018年中报负债率小于30%'],
        ['yysr_g2018_2_1','>',20,0,'营业收入2018年中报一年增长率大于20%'],
        ['jlr_g2018_2_1','>',20,0,'净利润2018年中报一年增长率大于20%'],
        ]

    stk6=xgtj(stk6,tjarr6)
    tjdf6=pd.DataFrame(tjarr6,columns=tjsm)
    stk6.to_excel(writer, sheet_name='选股六')  
    tjdf6.to_excel(writer, sheet_name='选股六参数',index=False)  


#    stk7=df.copy()

#    tjarr7=[
#        ['ssrq','<','20170701',0,'上市日期早于2015年7月1日'],
#        ['gdhsjb','<',5,0,'股东户数比上次不超过5%'],
#        ['gdhshb','<',10,0,'股东户数比上季度增长小于10%'],
#        ['jlr_g_bgq','>',5,0,'最新报告期净利润同比增长大于5%'],
#        ['zyts','包含','增持',0,'重要提示中包含【增持】'],
#        ['zjhhy','包含','银行',0,'证监会行业包含【银行】'],
#        ['zyts','不包含','减持',0,'重要提示中不包含【减持】'],
#        ['pe_ttm','<',30,0,'滚动市盈率小于30'],
#        ['roe2017','>',10,0,'2017年ROE大于15%'],
#        ['jll2017','>',15,0,'2017年净利率大于15%'],
#        ['hjsz','>',10,0,'户均市值大于10万元'],
#        ['fzl_2018.06.30','<',30,0,'2018年中报负债率小于30%'],
#        ['yysr_g2018_2_1','>',20,0,'营业收入2018年中报一年增长率大于20%'],
#        ['jlr_g2018_2_1','>',20,0,'净利润2018年中报一年增长率大于20%'],
#        ]

#    stk7=xgtj(stk7,tjarr7)
#    tjdf6=pd.DataFrame(tjarr6,columns=tjsm)
#    stk7.to_excel(writer, sheet_name='银行股')  
#    tjdf6.to_excel(writer, sheet_name='选股六参数',index=False)  

    writer.save()

    
    now3 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now1)
    print('下载完成时间：%s' % now2)
    print('结束运行时间：%s' % now3)

'''
    tjarr=[['ssrq','<','20150701',0,'上市日期早于2015年7月1日'],
        ['gdhsjb','<',5,0,'股东户数比上次不超过5%'],
        ['gdhshb','<',-10,0,'股东户数比上季度减少-10%以上'],
        ['jlr_g_bgq','>',10,0,'最新报告期净利润同比增长大于10%'],
        ['roe2017','>',10,0,'2017年ROE大于10%'],
        ['jll2017','>',10,0,'2017年净利率大于10%'],
        ['zyts','包含','增持',0,'重要提示中包含【增持】'],
        ['zyts','不包含','减持',0,'重要提示中不包含【减持】'],
        ['yysr2017','>','yysr2016',1,'2017年营业收入大于2016年营业收入'],
        ['pe_ttm','<',100,0,'滚动市盈率小于100'],
        ['roe2017','>',10,0,'2017年ROE大于10%'],
        ['jll2017','>',10,0,'2017年净利率大于10%'],
        ['hjsz','>',15,0,'户均市值大于15万元'],
        ['fzl_2018.06.30','<',30,0,'2018年中报负债率小于30%'],
        ['yysr_g2018_2_1','>',20,0,'营业收入2018年中报一年增长率大于20%'],
        ['jlr_g2018_2_1','>',20,0,'净利润2018年中报一年增长率大于20%'],
        ['y1_zf','>',10,0,'股价1年涨幅大于10%'],
        ]


    tjarr3=[
        ['zyts','包含','增持',0,'重要提示中包含【增持】'],
        ['zyts','不包含','减持',0,'重要提示中不包含【减持】'],
        ['yysr2017','>','yysr2016',1,'2017年营业收入大于2016年营业收入'],
        ['roe2017','>',10,0,'2017年ROE大于10%'],
        ['jll2017','>',10,0,'2017年净利率大于10%'],
        ['hjsz','>',15,0,'户均市值大于15万元'],
        ['fzl_2018.06.30','<',30,0,'2018年中报负债率小于30%'],
        ['yysr_g2018_2_1','>',20,0,'营业收入2018年中报一年增长率大于20%'],
        ['jlr_g2018_2_1','>',20,0,'净利润2018年中报一年增长率大于20%'],
        ['y1_zf','>',10,0,'股价1年涨幅大于10%'],
        ]


    stk5=df.copy()

    stk5=xgtj(stk5,tjarr)
'''