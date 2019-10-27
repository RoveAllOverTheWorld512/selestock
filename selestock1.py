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
from configobj import ConfigObj
import sqlite3
import numpy as np
import pandas as pd
from pandas.compat import StringIO
import xlwings as xw
import struct
import winreg
from selenium import webdriver
import time
import tushare as ts
from urllib import request
import requests
import zipfile
from bs4 import BeautifulSoup as bs


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
def iniconfig():
    inifile = os.path.splitext(sys.argv[0])[0]+'.ini'  #设置缺省配置文件
    return ConfigObj(inifile,encoding='GBK')

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

    terminator = f.read(1)

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
        start='20100101'

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
#获取最新交易日，如果当天是交易日，在18:00后用当天，如果当天不是交易日
###############################################################################
def lastday():
    config = iniconfig()
    stockclosedate = readkey(config,'stockclosedate')
    now = datetime.datetime.now()
    td = now.strftime("%Y%m%d") #今天
    hr = now.strftime("%H") #今天
    if hr<'16' :
        td = nextdtstr(td,-1)

    wk = str2datetime(td).weekday()
    if wk<5 and not td in stockclosedate :
        return td
    else :
        while True :
            td = nextdtstr(td,-1)
            wk = str2datetime(td).weekday()
            if wk<5 and not td in stockclosedate :
                return td


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
#获取本机通达信安装目录，生成自定义板块保存目录
########################################################################
def dlday():
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
            if ltime >= lastday() :
                #必须是最后一个交易日16点以后下载的最新数据
                if ltime == lastday() and time.strftime("%H",time.localtime(mtime))<'16':
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
def dlday1():
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

        dlyes = False    #下载标志，True表示要下载
        zip_file = svdir + "\\" + fn
        url = url0 + fn
        if os.path.exists(zip_file):
            
            mtime=os.path.getmtime(zip_file)  #文件建立时间
            ltime=time.strftime("%Y%m%d",time.localtime(mtime))
            if ltime >= lastday() :
                dlyes = False
            else :
                dlyes = True
        else :
            dlyes = True


        if dlyes:
            print ("正在下载的文件%s，请等待！" % zip_file)

            r = requests.get(url)
            #如果下载文件不存在 ，r返回 <Response [404]>， r.ok为False
            #如果下载文件存在 ，r返回 <Response [200]>，r.ok为True
            if not r.ok :
                print ("你所下载的文件%s不存在！" % zip_file)

            else :
                if os.path.exists(zip_file):
                    os.remove(zip_file)
                
                with open(zip_file, "wb") as f:
                    f.write(r.content)
                    f.close()


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

    return df.set_index('gpdm')
    
###############################################################################
#同花顺行业分类
###############################################################################
def thshy():
    dbfn=getdrive()+'\\hyb\\STOCKHY.db'
    cn = sqlite3.connect(dbfn)
    thshyfn=ths_dl_xls()
    wb = xw.Book(thshyfn)
    wb.sheets[0].range('A1').value="gpdm"
    wb.sheets[0].range('B1').value="gpmc"
    wb.sheets[0].range('C1').value="thshy"
    data=wb.sheets[0].range('A1').options(pd.DataFrame, expand='table').value
    xw.apps[0].quit()
    #将索引股票代码加为一列
    data['gpdm']=data.index
    #提取行业、去重、排序、编码
    hydm=data.loc[:,'thshy']
    hydm=hydm.drop_duplicates()
    hydm=hydm.sort_values()
    hydmdf=hydm.to_frame()
    #编码    
    hydmdf['zldm']="THS"
    hydmdf['hydm']=""
    i=1
    for index, row in hydmdf.iterrows():
        hydmdf.loc[index,'hydm']=i
        i += 1
    hydmdf=hydmdf.set_index('hydm',drop=False)

    hydmdf=hydmdf.loc[:,['zldm','hydm','thshy']]    
    hydmlst=hydmdf.values.tolist()
    
    cn.executemany('INSERT OR IGNORE INTO FLDM (ZLDM,FLDM,FLMC) VALUES (?,?,?)', hydmlst)
    cn.commit()
    
    #将股票行业编码化    
    gphydf=pd.merge(data,hydmdf,on='thshy')  
    gphydf=gphydf.loc[:,['gpdm','zldm','hydm']]    
    gphylst=gphydf.values.tolist()
    cn.executemany('INSERT OR IGNORE INTO GPFLDM (GPDM,ZLDM,FLDM) VALUES (?,?,?)', gphylst)
    cn.commit()

    cn.close()

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
#从同花顺i问财下载,prefix:eps、growth、rpt
###############################################################################
def dl_ths_xls(prefix):

    config = iniconfig()
    
    ddir=os.path.join(getdrive(),readkey(config,'dldir'))
    dafn = dlfn(ddir)

    nf1 = int(readkey(config,prefix + 'nf1'))
    nf2 = int(readkey(config,prefix + 'nf2'))
    nb = readkey(config, prefix + 'rq')
    kw0 = readkey(config, prefix + 'kw')
    sele = readkey(config, prefix + 'sl')
    newfn0 = readkey(config, prefix + 'fn')

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
    
    for j in range(nf1,nf2+1):

        kw = str(j) + nb + kw0
        newfn = newfn0 + str(j) + '.xls'
        newfn = os.path.join(ddir,newfn)
        
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

#########################################################################
#读取键值
#########################################################################
def readkey(config,key):
    keys = config.keys()
    if keys.count(key) :
        return config[key]
    else :
        return ""

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
    return os.path.splitdrive(sys.argv[0])[0]
#def getdrive():
#    return sys.argv[0][:2]


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
#计算PE、PEG
##########################################################################   
def calc_peg(wb):

    opened = False
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
##########################################################################
def get_stock_basics():
    #http://file.tushare.org/tsdata/all.csv
    csvfn = r'D:\selestock\all.csv'
    text=open(csvfn,'rb').read()
    text = text.decode('GBK')
    text = text.replace('--', '')
    df = pd.read_csv(StringIO(text), dtype={'code':'object'})
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
#根据业绩预告计算PEG选股
##########################################################################
def xg_ygpeg():    
#    dl_iwencaixls2()
    if not joinsht():
        print("请检查selestock1.xls和selestock2.xls栏目，有重复项目。工作无法继续，退出！")
        sys.exit()

    fldd = field_dic()
    
    gpssrq=get_ssrq()
    gpdmb=get_gpdm()
    jbxx=gpdmb.join(gpssrq)
    
    #筛选2013年1月1日前上市的生成股票池
    ss=gpssrq[(gpssrq['ssrq']<'20130101')]
    pool_0 = ss.index
   
    #筛选2015和2016年营业收入和净利润都增长的生成股票池
    pool_1 = stkpool_grth()
    
    #筛选2015和2016年roe>10的生成股票池
    pool_2 = stkpool_roe()
    
    #筛选2016年商誉净资产占比>40的生成股票池
#    pool_3 = stkpool_syjzczb()
    
    #筛选2015和2016年基本eps增长率>0,eps>0
    pool_4 = stkpool_eps0_g()

    #检查有没有打开选股分析.xlsm文档
    opened = False
    for bk in xw.books:
        if bk.name == '选股分析.xlsm' :
            opened = True
            wb=bk
    if not opened :
        wb = xw.books.open(r'D:\selestock\选股分析.xlsm')

    wb.activate()        
    sht = wb.sheets('selestock')    
    sht.activate()

    df = sht.range('A1').options(pd.DataFrame, expand='table').value
    df.columns=[e[0] for e in df.columns]

    cols = df.columns

    colse = [fldd[e] for e in cols] 
    df.columns = colse

    #转换NaN
    df=df.replace('--',np.nan) 
    
    #计算预告eps
    df = df.assign(ygeps0 = df['ygjlr']/df['zgb'])

    #计算pe
    df = df.assign(ygpe = df['gj']/df['ygeps0'])

    #计算预告peg    
    df = df.assign(ygpeg = df['ygpe']/df['ygjlr_g'])

    df = df.assign(pe = df['gj'] / df['eps0'])
    df = df.assign(peg = df['pe'] / df['eps0_g'])

    df.index.name='gpdm'
    df.drop('gpmc',axis=1, inplace=True)

    jbxx1=jbxx[jbxx.index.isin(df.index)]
    df1=jbxx1.join(df)
    
    sht = clearsheet('peg')
    
    sht.range('A1').value = df1
    

    '''筛选'''
    
    #转换上市日期，删去3年内上市的股票
    df =df[(df.index.isin(pool_0))]

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
    #去掉上年净利润亏损的,今年预告净利润亏损，净利润减少的
    df=df[(df['jlr_1']>0) & (df['ygjlr']>0) & (df['ygjlr_g']>0)]

    #去掉pe大于100
    df=df[(df['ygpe']<100)]
    
    #保留peg<1
    df=df[(df['ygpeg']>0) & (df['ygpeg']<1)]

    #保留peg<1
#    df=df[(df['peg']>0) & (df['peg']<1)]

#    df.index.name='gpdm'
#    df.drop('gpmc',axis=1, inplace=True)
    
    jbxx1=jbxx[jbxx.index.isin(df.index)]
    df=jbxx1.join(df)
    
    sht = clearsheet('result')
    
    sht.range('A1').value = df

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
#根据业绩
##########################################################################
def xg_peg():
    now = datetime.datetime.now().strftime('%Y%m%d_%H%M')

    #筛选2015和2016年营业收入和净利润都增长的生成股票池
    pool_1 = stkpool_grth()
    
    #筛选2015和2016年roe>10的生成股票池
    pool_2 = stkpool_roe()
    
    #筛选2016年商誉净资产占比>40的生成股票池
    pool_3 = stkpool_syjzczb()
    
    #筛选2015和2016年基本eps增长率>0,eps>0
    pool_4 = stkpool_eps0_g()

    #筛选2016年基本eps增长率>0,eps>0
#    pool_5 = stkpool_2016()
    #上市日期
    gpssrq=get_ssrq()
    #股票代码、名称、拼音
    gpdmb=get_gpdm()
    #股票申万行业
    gpswhy=get_swhy()
    #股票商誉占比
    gpsyzb=get_syzb()
    
    #最新股东户数
    gpgdhs=get_gdhs()
    
    #信息合并
    jbxx=gpdmb.join(gpssrq)
    jbxx=jbxx.join(gpswhy)
    jbxx=jbxx.join(gpsyzb)
    jbxx=jbxx.join(gpgdhs)
    
    #提取股价
    df=ts.get_today_all()
    df['gpdm']=df['code'].map(lambda x:x+('.SH' if x[0]=='6' else '.SZ'))
    df=df[['gpdm','trade']]
    df=df.set_index('gpdm')

    #2017年业绩
    nf=2017
    yjyg=get_yjyg(nf)    
    yjbb=get_yjbb(nf)
    yjkb=get_yjkb(nf)
    yj=pd.concat([yjbb,yjkb,yjyg])
    yj=yj[~yj.index.duplicated()]
    
    yj=yj.replace('-',np.nan) 

    #关联基本信息
    jbxx1=jbxx[jbxx.index.isin(yj.index)]
    df=jbxx1.join(df)
    df=df.join(yj)

    #计算PE
    df=df.assign(pe = df['trade']/df['eps'])
    #eps小于等于0,PE无意义
    df.loc[df['eps']<=0,'pe']=np.nan
    #股价为0,停牌
    df.loc[df['trade']<=0,'pe']=np.nan
    
    #计算PEG
    df=df.assign(peg = df['pe']/df['jlr_g'])
 
    #净利润同比增长率为负的，PEG也没有意义
    df.loc[df['jlr_g']<=0,'peg']=np.nan
    
    #上一年净利润为负，净利润同比增长率也没有意义
    df.loc[df['jlr_1']<0,'peg']=np.nan

    #计算pb
    df=df.assign(pb = df['trade']/df['mgjzc'])
    df.loc[df['mgjzc']<=0,'pb']=np.nan
    df.loc[df['trade']<=0,'pb']=np.nan
    

    df=df.round(2)

    
    #选股
    #PE小于30
    df1=df[(df['pe']<30)]

    #PEG小于1
    df1=df1[(df1['peg']<1.1)]

    #营业收入增长
    df1=df1[~(df1['yysr_g']<0)]    
    
    #上市时间
    df1=df1[(df1['ssrq']<'20160101')]
    
    #股东户数同比减少
    df1=df1[(df1['gdhstb']<0)]

    #股东户数季度环比不大于
    df1=df1[~(df1['gdhshb']>5)]

    #股价小于60
    df1=df1[(df1['trade']<60)]

    #筛选roe和成长性   
    #筛选2015和2016年营业收入和净利润都增长的生成股票池
    df1 =df1[(df1.index.isin(pool_1))]
    
    #筛选2015和2016年roe>10的生成股票池
    df1 =df1[(df1.index.isin(pool_2))]

    #删除商誉占比高德 ,注意pandas的取反运算符~   
#    df1 = df1[(~df1.index.isin(pool_3))]

    #eps增长率
    df1 =df1[(df1.index.isin(pool_4))]
    

    df1['zf1y']=df1.index.map(lambda x:x[:6]+'_20170305_20180305').map(get_zf)
    df1['zf2y']=df1.index.map(lambda x:x[:6]+'_20160305_20180305').map(get_zf)
    df1['zf3m']=df1.index.map(lambda x:x[:6]+'_20180101_20180308').map(get_zf)
    
    #2016年关键财务指标
#    df1 =df1[(df1.index.isin(pool_5))]

    fn = r'd:\selestock\peg'+now+'.xlsx'

    if os.path.exists(fn):
        os.remove(fn)

    writer=pd.ExcelWriter(fn,engine='xlsxwriter')

    df.to_excel(writer, sheet_name='业绩快报')   
    df1.to_excel(writer, sheet_name='选股')  
    writer.save()

    
    
##########################################################################
#根据净利润经营现金流含量选股
##########################################################################
def xg_yjbb():

    fn = r'd:\selestock\yjbb2017.xlsx'

    if os.path.exists(fn):
        os.remove(fn)

    #筛选2015和2016年营业收入和净利润都增长的生成股票池
    pool_1 = stkpool_grth()
    
    #筛选2015和2016年roe>10的生成股票池
    pool_2 = stkpool_roe()
    
    #筛选2016年商誉净资产占比>40的生成股票池
    pool_3 = stkpool_syjzczb()
    
    #筛选2015和2016年基本eps增长率>0,eps>0
    pool_4 = stkpool_eps0_g()
    
    gpssrq=get_ssrq()
    gpdmb=get_gpdm()
    jbxx=gpdmb.join(gpssrq)
    #提取股价
    df=ts.get_today_all()
    df['gpdm']=df['code'].map(lambda x:x+('.SH' if x[0]=='6' else '.SZ'))
    df=df[['gpdm','trade']]
    df=df.set_index('gpdm')

    yjbb=get_yjbb(2017)
    yjbb=yjbb.replace('-',np.nan) 

    
    jbxx1=jbxx[jbxx.index.isin(yjbb.index)]
    df=jbxx1.join(df)
    df=df.join(yjbb)
   
    df=df.assign(pe = df['trade']/df['eps'])
    #eps小于等于0,PE无意义
    df.loc[df['eps']<=0,'pe']=np.nan
    df.loc[df['trade']<=0,'pe']=np.nan
    
    df=df.assign(peg = df['pe']/df['jlr_g'])
 
    #净利润同比增长率为负的，PEG也没有意义
    df.loc[df['jlr_g']<=0,'peg']=np.nan
    
    df=df.assign(pb = df['trade']/df['mgjzc'])

    df=df.round(2)
    #选股
    
    df1=df[((df['peg']<1) & (df['yysr_g']>0) & (df['ssrq']<'20140101'))]
    #营业收入增长

    #筛选roe和成长性   
    df1 =df1[(df1.index.isin(pool_1))]
    df1 =df1[(df1.index.isin(pool_2))]

    #删除商誉占比高德 ,注意pandas的取反运算符~   
#    df1 = df1[(~df1.index.isin(pool_3))]

    #eps增长率
    df1 =df1[(df1.index.isin(pool_4))]

    writer=pd.ExcelWriter(fn,engine='xlsxwriter')

    df.to_excel(writer, sheet_name='业绩报表')   
    df1.to_excel(writer, sheet_name='选股')  
    writer.save()


##########################################################################
#获取最新股东户数及季度环比变化
##########################################################################
def get_gdhs():
    dbfn=getdrive()+'\\hyb\\STOCKDATA.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    
    #提取年报以来最新股东户数    
    curs.execute('''select gpdm,rq,gdhs from gdhs 
              where rq>='2017-12-31' order by rq desc;''')
    
    data = curs.fetchall()
    
    #保留最新户数
    df0=pd.DataFrame(data,columns=['gpdm','rq0','gdhs0'])
    df0 = df0.drop_duplicates(['gpdm'],keep='first')
    
    #
    df1=pd.DataFrame(data,columns=['gpdm','rq0','gdhs0'])
    df1=df1.append(df0)
    
    #下一句很关键
    #Python Dataframe ——指定多列去重、求差集
    #https://blog.csdn.net/jasonwang_/article/details/78797458
    
    df1 = df1.drop_duplicates(keep=False)
    df1 = df1.drop_duplicates(['gpdm'],keep='first')
    df1.columns=['gpdm','rq1','gdhs1']

    df0 = df0.set_index('gpdm')
    df1 = df1.set_index('gpdm')
        
    curs.execute('''select gpdm,rq,gdhs from gdhs 
              where rq=='2017-12-31';''')
    data = curs.fetchall()
    
    df2=pd.DataFrame(data,columns=['gpdm','rq2','gdhs2'])
    df2=df2.set_index('gpdm')

    curs.execute('''select gpdm,rq,gdhs from gdhs 
              where rq=='2016-12-31';''')
    data = curs.fetchall()
    
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
#提取指定日期股票的前后3个交易日信息
########################################################################
def get_k_data(gpdm,start=None,end=None,autype='qfq'):
    
    gpdm=sgpdm(gpdm)

    ssrq=get_ssrq().loc[lgpdm(gpdm),'ssrq']
    
    start= ssrq if (start==None or start<ssrq) else start
    end= datetime.datetime.now().strftime('%Y%m%d') if end==None else end
    
    #检测日期有效性，无效则退出,返回一个空的DF    
    date1=str2datetime(start)
    date2=str2datetime(end)
    
    if date1==None or date2==None :
        columns = ['date','open', 'high', 'low','close','amout','volume','rate','pre_close']
        data = []
        df = pd.DataFrame(columns=columns)
        df = df.set_index('date')
        return df
    
    start=date1.strftime("%Y%m%d")
    end=date2.strftime("%Y%m%d")
                
    sc = 'sh' if gpdm[0]=='6' else 'sz'

#    dayfn =gettdxdir()+'\\vipdoc\\'+sc+'\\lday\\'+sc+gpdm+'.day'
    dayfn =getdrive()+'\\tdx\\'+sc+'lday\\'+sc+gpdm+'.day'
#    dayfn ='d:\\pegtest\\vipdoc\\'+sc+'\\lday\\'+sc+gpdm+'.day'
        
    if not os.path.exists(dayfn):
        
        columns = ['date','open', 'high', 'low','close','amout','volume','rate','pre_close']
        data = []
        df = pd.DataFrame(columns=columns)
        df = df.set_index('date')
        
        return df
    
    with open(dayfn,"rb") as f:
        data = f.read()
        f.close()
    days = int(len(data)/32)
    
    records = []
    qsp = np.nan
    for i in range(days):
        dat = data[i*32:(i+1)*32]
        rq,kp,zg,zd,sp,cje,cjl,tmp = struct.unpack("iiiiifii", dat)
        if rq<int(start):
            qsp = sp/100.00
            continue

        kp = kp/100.00
        zg = zg/100.00
        zd = zd/100.00
        sp = sp/100.00
        cje = cje/100000000.00     #亿元
        cjl = cjl/10000.00         #万股
        
        records.append([rq,kp,zg,zd,sp,cje,cjl])
        qsp = sp
        
        if rq>int(end):
            break

    columns = ['date','open', 'high', 'low','close','amout','volume']
    
    df = pd.DataFrame(records,columns=columns)
    df = df.set_index('date')
    df.index=df.index.map(lambda x:str(x))
    
    df=df.sort_index(ascending=True)
    
    #不复权
    if autype==None:
        return df.round(4)
            
    #前复权、后复权
    date1=df.index[0]
    date2=df.index[len(df)-1]
    
    #提取前复权因子
    yzdf=get_adjf(gpdm,date1,date2,autype)
    #关联    
    df=df.join(yzdf)
    #计算
    df.eval('open = open*ljyz',inplace=True)
    df.eval('high = high*ljyz',inplace=True)
    df.eval('low = low*ljyz',inplace=True)
    df.eval('close = close*ljyz',inplace=True)

    #删除前复权因子
    df.drop('ljyz',axis=1, inplace=True)    
    
    return df.round(4)


########################################################################
#提取质押率
########################################################################
def get_zyl():

    '''
    读取i问财下载的xls文件：最新负债率

    '''
    
    xlsfn = 'd:\\selestock\\zyl.xls'
    coldic={'股票代码':'gpdm',	
        '公司出质人累计质押比例合计(%)'	:'zyl',
        '公司出质人质押占总股本比合计(%)':'zyzgbzb'
        }
    
    colfloat=['zyl']
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
    读取i问财下载的xls文件：最新负债率

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

    df=df.set_index(keycol)

    
    return df
    
    

########################################################################
#提取负债率
########################################################################
def get_fzl():

    '''
    读取i问财下载的xls文件：最新负债率
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

########################################################################
#提取指定日期股票的前后2个交易日信息
########################################################################
def get_trade(gpdm,date):
    
    gpdm=sgpdm(gpdm)
    
    #检测日期有效性，无效则退出    
    date0=str2datetime(date)
    if date0==None :
        return None
    
    date=date0.strftime("%Y%m%d")
                
    sc = 'sh' if gpdm[0]=='6' else 'sz'

#    dayfn =gettdxdir()+'\\vipdoc\\'+sc+'\\lday\\'+sc+gpdm+'.day'
    dayfn =getdrive()+'\\tdx\\'+sc+'lday\\'+sc+gpdm+'.day'
#    dayfn ='d:\\pegtest\\vipdoc\\'+sc+'\\lday\\'+sc+gpdm+'.day'
        
    if not os.path.exists(dayfn):
        
        columns = ['date','open', 'high', 'low','close','amout','volume','rate','pre_close']
        data = [int(date),np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan]
        df = pd.DataFrame([data],columns=columns)
        df = df.set_index('date')
        
        return df
    
    with open(dayfn,"rb") as f:
        data = f.read()
        f.close()
    days = int(len(data)/32)
    
    #找到日期起始位置再前移3个交易日
    for i in range(days):
        dat = data[i*32:(i+1)*32]
        rq,kp,zg,zd,sp,cje,cjl,tmp = struct.unpack("iiiiifii", dat)
        if rq<int(date):
            continue
        if rq>=int(date):
            break

    s1=i-1
    s2=i+1
    if s1<0:
        s1=0
    if s2>days:
        s2=days
        
    #读取数据
    records = []
    qsp = np.nan
    for i in range(s1,s2):
        dat = data[i*32:(i+1)*32]
        rq,kp,zg,zd,sp,cje,cjl,tmp = struct.unpack("iiiiifii", dat)

        kp = kp/100.00
        zg = zg/100.00
        zd = zd/100.00
        sp = sp/100.00
        cje = cje/100000000.00     #亿元
        cjl = cjl/10000.00         #万股
        zf = (sp/qsp-1)*100
        
        records.append([rq,kp,zg,zd,sp,cje,cjl,zf,qsp])
        qsp = sp

    columns = ['date','open', 'high', 'low','close','amout','volume','rate','pre_close']
    
    df = pd.DataFrame(records,columns=columns)
    df = df.set_index('date')
    df.index=df.index.map(lambda x:str(x))
    
    return df.round(2)

########################################################################
#提取指定日期股票的交易信息：gpdm_date-->'300340_20160913'
########################################################################
def get_tradeinfo(gpdm_date):
    
    p='(\d{6}).+(\d{8})'
    para=re.findall(p,gpdm_date)
    gpdm=para[0][0]
    date=para[0][1]

    sc = 'sh' if gpdm[0]=='6' else 'sz'

#    dayfn =gettdxdir()+'\\vipdoc\\'+sc+'\\lday\\'+sc+gpdm+'.day'
#    dayfn ='d:\\pegtest\\vipdoc\\'+sc+'\\lday\\'+sc+gpdm+'.day'
    dayfn =getdrive()+'\\tdx\\'+sc+'lday\\'+sc+gpdm+'.day'
        
    if not os.path.exists(dayfn):
        return [gpdm,int(date),np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan]
        
    with open(dayfn,"rb") as f:
        data = f.read()
        f.close()
    days = int(len(data)/32)
    qsp = np.nan
    for i in range(days):
        dat = data[i*32:(i+1)*32]
        rq,kp,zg,zd,sp,cje,cjl,tmp = struct.unpack("iiiiifii", dat)
        if rq<int(date):
            qsp = sp
            continue
        if rq>=int(date):
            break

    kp = kp/100.00
    zg = zg/100.00
    zd = zd/100.00
    sp = sp/100.00
    cje = cje/100000000.00     #亿元
    cjl = cjl/10000.00         #万股
    qsp = qsp/100.00
    zf = (sp/qsp-1)*100
    
    if rq==int(date):
        return [gpdm,rq,kp,zg,zd,sp,cje,cjl,qsp,zf]
    
    return [gpdm,int(date),np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan]

##########################################################################
#查询指定区间前复权因子
##########################################################################
def query_adjf(dm,date1,date2):
    dbfn=getdrive()+'\\hyb\\STOCKDATA.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    gpdm=lgpdm(dm)
    sql = 'select gqrq,ljqfqyz,yzjzrq from adjfactor where gpdm=? ;'
    
    curs.execute(sql,(gpdm,))
    
    data = curs.fetchall()
    dbcn.close()
    
    cols = ['gqrq','ljqfqyz','jzrq']
    if len(data)>0:
        df = pd.DataFrame(data,columns=cols)
        df = df.set_index('gqrq')
        df = df.sort_index(ascending=True)
        #下句很重要，对于整列全缺失的情况
        df['ljqfqyz'] = df['ljqfqyz'].astype(np.float64)
        
        rqlst=df.index
        
        yzall = []
        for i in range(1,len(df)):
            d1=strnextdate(rqlst[i-1],1)
            d2=rqlst[i]
            rng=pd.bdate_range(d1,d2)
            yz=pd.DataFrame(df.iloc[i]['ljqfqyz'],columns=['ljyz'],index=rng)
            
            yzall.append(yz)
        
        if date1<rqlst[0] :
            d1=date1
            d2=rqlst[0]
            rng=pd.bdate_range(d1,d2)
            yz=pd.DataFrame(df.iloc[0]['ljqfqyz'],columns=['ljyz'],index=rng)
            yzall.append(yz)
            
        if date2>rqlst[len(df)-1]:
            d1=strnextdate(rqlst[len(df)-1],1)
            d2=date2
            rng=pd.bdate_range(d1,d2)
            yz=pd.DataFrame(1,columns=['ljyz'],index=rng)
            yzall.append(yz)
    
        yzdf=pd.concat(yzall)
        yzdf.index=yzdf.index.map(lambda x:x.strftime('%Y%m%d'))
    
        '''下面这条语句很重要'''
        yzdf=yzdf.sort_index(ascending=True)
            
        return yzdf[date1:date2]  

    else:

        rng=pd.bdate_range(date1,date2)
        yzdf=pd.DataFrame(1,columns=['ljyz'],index=rng)
        yzdf.index=yzdf.index.map(lambda x:x.strftime('%Y%m%d'))

        return yzdf

    
##########################################################################
#查询指定区间复权因子,autype:qfq\hfq
##########################################################################
def get_adjf(dm,date1=None,date2=None,autype='qfq'):

    gpdm=lgpdm(dm)
    #提取股票上市日期    
    ssrq=get_ssrq().loc[gpdm,'ssrq']
    
    date1= ssrq if (date1==None or date1<ssrq) else date1
    date2= datetime.datetime.now().strftime('%Y%m%d') if date2==None else date2
        
    dbfn=getdrive()+'\\hyb\\STOCKDATA.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    sql = 'select gqrq,ljqfqyz,yzjzrq from adjfactor where gpdm=? ;'
    
    curs.execute(sql,(gpdm,))
    
    data = curs.fetchall()
    dbcn.close()
    
    cols = ['gqrq','ljqfqyz','jzrq']
    if len(data)>0:
        df = pd.DataFrame(data,columns=cols)
        df = df.set_index('gqrq')
        df = df.sort_index(ascending=True)
        #下句很重要，对于整列全缺失的情况
        df['ljqfqyz'] = df['ljqfqyz'].astype(np.float64)
        
        rqlst=df.index
        
        yzall = []
        for i in range(1,len(df)):
            d1=strnextdate(rqlst[i-1],1)
            d2=rqlst[i]
            rng=pd.bdate_range(d1,d2)
            yz=pd.DataFrame(df.iloc[i]['ljqfqyz'],columns=['ljyz'],index=rng)
            
            yzall.append(yz)
        
        if date1<rqlst[0] :
            d1=date1
            d2=rqlst[0]
            rng=pd.bdate_range(d1,d2)
            yz=pd.DataFrame(df.iloc[0]['ljqfqyz'],columns=['ljyz'],index=rng)
            yzall.append(yz)
            
        if date2>rqlst[len(df)-1]:
            d1=strnextdate(rqlst[len(df)-1],1)
            d2=date2
            rng=pd.bdate_range(d1,d2)
            yz=pd.DataFrame(1,columns=['ljyz'],index=rng)
            yzall.append(yz)
    
        yzdf=pd.concat(yzall)
        yzdf.index=yzdf.index.map(lambda x:x.strftime('%Y%m%d'))
    
    else:

        rng=pd.bdate_range(date1,date2)
        yzdf=pd.DataFrame(1,columns=['ljyz'],index=rng)
        yzdf.index=yzdf.index.map(lambda x:x.strftime('%Y%m%d'))

    '''下面这条语句很重要'''
    yzdf=yzdf.sort_index(ascending=True)
    yzdf=yzdf[date1:date2]

    if autype=='hfq':
        yzdf['hfqyz']=1

        for i in range(1,len(yzdf)):
            a=yzdf.iloc[i]['ljyz']/yzdf.iloc[i-1]['ljyz']
            b=yzdf.iloc[i-1]['hfqyz']
            yzdf.iloc[i,yzdf.columns.get_loc('hfqyz')]=b*a

        yzdf['ljyz']=yzdf['hfqyz']    
        
    return yzdf[['ljyz']]
    
            
##########################################################################
#最后分红配股日期
##########################################################################
def get_adjf_lastdate():
    dbfn=getdrive()+'\\hyb\\STOCKDATA.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()

    sql = 'select gpdm,max(gqrq) as rq  from adjfactor group by gpdm;'
    curs.execute(sql)
    
    data = curs.fetchall()
    dbcn.close()
    
    cols = ['gpdm','fqjzrq']
    
    df = pd.DataFrame(data,columns=cols)
    df = df.set_index('gpdm')
    
    return df['fqjzrq']
    
##########################################################################
#日期字符串，指定日期后days天，days为负表示前days天
##########################################################################
def strnextdate(date,days):
    
    return (str2datetime(date) + datetime.timedelta(days)).strftime("%Y%m%d")
    
##########################################################################
#获取指定日期的前复权股价，flag=0表示模糊检索，返回的为pandas Series
#flag:0表示在找不到指定日期值时，找这个日期前一个交易日数据，1则取后一个
##########################################################################
def get_tradeinfo_adjclose(dm,date,flag=0):

    jyxx=get_trade(dm,date)
    #下面3条语句是原来自己计算前复权因子的
#    date1=jyxx.index[0]
#    date2=jyxx.index[len(jyxx)-1]    
#    yzdf=query_adjf(dm,date1,date2)
    
    #下面3条语句是使用tushare复权因子
    yzdf=factor_adj(dm)   
    yzdf.index=yzdf.index.map(lambda x:x.replace('-','')) 
    yzdf.columns=['ljyz']   #将adj_factor改名
    
    jyxx=jyxx.join(yzdf)
    jyxx.eval('adjclose = close*ljyz',inplace=True)
    
    if date in jyxx.index:
        return jyxx.loc[date]
    elif flag==0 :
        return jyxx.iloc[0]
    elif flag==1:
        return jyxx.iloc[len(jyxx)-1]        
    else:
        data=[np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan]
        index=['open', 'high', 'low', 'close', 'amout', 'volume', 'rate', 'pre_close',
       'ljyz', 'adjclose']
        return pd.Series(data,index=index,name=date)

##########################################################################
#日期字符串，指定日期后days天，days为负表示前days天
##########################################################################
def get_close(dm_date):
    return get_tradeinfo(dm_date)[5]

##########################################################################
#日期字符串，指定日期后days天，days为负表示前days天
##########################################################################
def get_adj_close(gpdm_date):

    #最近的分红配股日期    
    fqjzrq=get_adjf_lastdate()

    p='(\d{6}).+(\d{8})'
    para=re.findall(p,gpdm_date)
    gpdm=para[0][0]
    date=para[0][1]
    
    #没有分红配股    
    if not (lgpdm(gpdm) in fqjzrq.index):
        return get_close(gpdm_date)
    
    #日期后有分红配股，需要复权
    if fqjzrq[lgpdm(gpdm)]>=date:
        jyxx=get_tradeinfo_adjclose(gpdm,date,1)
        return jyxx['adjclose']
    else:
        return get_close(gpdm_date)
    
##########################################################################
#日期字符串，指定日期后days天，days为负表示前days天
##########################################################################
def get_all_adjclose(date):
    gpdmb=get_gpdm()

    gpdmb['gpdm']=gpdmb.index.map(lambda x:x[:6])
    
    gpdmb['gj']=gpdmb['gpdm'].map(lambda x:x+'_'+date).map(get_adj_close)
    
    return gpdmb['gj']

##########################################################################
#获取股票基本信息：代码、名称、拼音、行业、商誉占比、股东户数及变化、当前股价
##########################################################################
def get_jbxx():
    
    stkinfo=get_stock_basics()
    stkinfo['gpdm']=stkinfo.index.map(lambda x:x+('.SH' if x[0]=='6' else '.SZ'))
    stkinfo['ssrq']=stkinfo['timeToMarket'].map(lambda x:str(x) if x>0 else '')
    gpssrq=stkinfo[['gpdm','ssrq','industry']]
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
    
    #负债率
    gpfzl=get_fzl()
    gpdxfzl=get_dxfzl()
    
    #质押率
    gpzyl=get_zyl()

    #信息合并
    jbxx=gpdmb.join(gpssrq)
    jbxx=jbxx.join(gpswhy)
    jbxx=jbxx.join(gptdxgfz)
    jbxx=jbxx.join(gpsyzb)
    jbxx=jbxx.join(gpgdhs)
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

    #提取股价
    gpgj=get_todaygj()
    
    #信息合并
    jbxx=jbxx.join(gpgj)
    jbxx=jbxx.fillna(value={'zyts':''})

    return jbxx    

##########################################################################
#区间涨幅统计
##########################################################################
def get_zf(dm_date1_date2):
    print(dm_date1_date2)
    abc=dm_date1_date2.split('_')
    dm=abc[0]
    date1=abc[1]
    date2=abc[2]
    jy1=get_tradeinfo_adjclose(dm,date1)
    jy2=get_tradeinfo_adjclose(dm,date2)
    zf=(jy2.adjclose/jy1.adjclose-1)*100
    
    return zf.round(2)

##########################################################################
#区间涨幅统计
##########################################################################
def get_zf1(dm_date1_date2):
    print(dm_date1_date2)
    abc=dm_date1_date2.split('_')
    dm=abc[0]
    date1=abc[1]
    date2=abc[2]
    jy1=get_tradeinfo_adjclose(dm,date1,1)
    jy2=get_tradeinfo_adjclose(dm,date2,1)
    zf=(jy2.adjclose/jy1.adjclose-1)*100
    
    return zf.round(2)

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
#根据2016年业绩和2017年业绩选股
##########################################################################
def xg_peg2():
    now = datetime.datetime.now().strftime('%Y%m%d_%H%M')

    #获取股票基本信息
    jbxx=get_jbxx()
    
    #2017年业绩
    yj=get_yj2017()

    #营业收入和净利润3年复合增长率
    jbxx['gpdm']=jbxx.index
    
    #2017年3季度3年复合增长率
    nf_jd_y='2017_3_1' 
    zzl0=jbxx['gpdm'].map(lambda x:x+'|'+nf_jd_y).map(get_g)
    zzl0=zzl0.apply(lambda x: pd.Series([x[0],x[1]]))
    zzl0.columns=['yysr_g'+nf_jd_y,'jlr_g'+nf_jd_y]
    jbxx=jbxx.join(zzl0)

    nf_jd_y='2017_3_2' 
    zzl1=jbxx['gpdm'].map(lambda x:x+'|'+nf_jd_y).map(get_g)
    zzl1=zzl1.apply(lambda x: pd.Series([x[0],x[1]]))
    zzl1.columns=['yysr_g'+nf_jd_y,'jlr_g'+nf_jd_y]
    jbxx=jbxx.join(zzl1)

    nf_jd_y='2017_3_3' 
    zzl2=jbxx['gpdm'].map(lambda x:x+'|'+nf_jd_y).map(get_g)
    zzl2=zzl2.apply(lambda x: pd.Series([x[0],x[1]]))
    zzl2.columns=['yysr_g'+nf_jd_y,'jlr_g'+nf_jd_y]
    jbxx=jbxx.join(zzl2)

    nf_jd_y='2017_3_5' 
    zzl3=jbxx['gpdm'].map(lambda x:x+'|'+nf_jd_y).map(get_g)
    zzl3=zzl3.apply(lambda x: pd.Series([x[0],x[1]]))
    zzl3.columns=['yysr_g'+nf_jd_y,'jlr_g'+nf_jd_y]
    jbxx=jbxx.join(zzl3)

    nf_jd_y='2017_3_7' 
    zzl4=jbxx['gpdm'].map(lambda x:x+'|'+nf_jd_y).map(get_g)
    zzl4=zzl4.apply(lambda x: pd.Series([x[0],x[1]]))
    zzl4.columns=['yysr_g'+nf_jd_y,'jlr_g'+nf_jd_y]
    jbxx=jbxx.join(zzl4)

    jbxx=jbxx.drop('gpdm',axis=1)

    
    #只提取有业绩信息的股票
    jbxx1=jbxx[jbxx.index.isin(yj.index)]
    df=jbxx1.join(yj)
    
    #计算PE
    df=df.assign(pe = df['gj']/df['eps'])
    #eps小于等于0,PE无意义
    df.loc[df['eps']<=0,'pe']=np.nan
    #股价为0,停牌
    df.loc[df['gj']<=0,'pe']=np.nan
    
    #计算PEG
    df=df.assign(peg = df['pe']/df['jlr_g'])
 
    #净利润同比增长率为负的，PEG也没有意义
    df.loc[df['jlr_g']<=0,'peg']=np.nan
    
    #上一年净利润为负，净利润同比增长率也没有意义
    df.loc[df['jlr_1']<0,'peg']=np.nan

    #计算pb
    df=df.assign(pb = df['gj']/df['mgjzc'])
    df.loc[df['mgjzc']<=0,'pb']=np.nan
    df.loc[df['gj']<=0,'pb']=np.nan

    #盈利预测
    ylyc=get_ylyc2018()
    df=df.join(ylyc)
    
    df=df.assign(eps1_g = (df['mean']/df['eps']-1)*100)
    df=df.assign(eps1_g_x = (df['amax']/df['eps']-1)*100)
    df=df.assign(eps1_g_n = (df['amin']/df['eps']-1)*100)

    df=df.round(2)

    td=datetime.datetime.now()
    nw=td.strftime("%Y%m%d")
    
    y1=(td+datetime.timedelta(-365)).strftime("%Y%m%d")
    y2=(td+datetime.timedelta(-365*2)).strftime("%Y%m%d")
    m6=(td+datetime.timedelta(-30*6)).strftime("%Y%m%d")
    m3=(td+datetime.timedelta(-30*3)).strftime("%Y%m%d")
    m1=(td+datetime.timedelta(-30*1)).strftime("%Y%m%d")
    
    
#    df['zf1y']=df.index.map(lambda x:x[:6]+'_'+y1+'_'+nw).map(get_zf)
#    df['zf2y']=df.index.map(lambda x:x[:6]+'_'+y2+'_'+nw).map(get_zf)
#    df['zf6m']=df.index.map(lambda x:x[:6]+'_'+m6+'_'+nw).map(get_zf)
#    df['zf3m']=df.index.map(lambda x:x[:6]+'_'+m3+'_'+nw).map(get_zf)
#    df['zf1m']=df.index.map(lambda x:x[:6]+'_'+m1+'_'+nw).map(get_zf)


    #选股说明
    xgsm=[['选股时间：',now]]
    #选股
    #PE小于30
    df1=df[(df['pe']<50)]
    xgsm.append(['市盈率：','小于50'])
    #净利润增长率大于12%
#    df1=df[((df['jlr_g']<50) & (df['jlr_g']>15))]
    df1=df1[(df1['jlr_g']>12)]
    xgsm.append(['净利润增长率：','在12%以上'])

    #PEG小于1
    df1=df1[(df1['peg']<1.5)]
    xgsm.append(['PEG：','小于1.5'])

    #ROE大于10
    df1=df1[~(df1['roe']<10)]
    xgsm.append(['ROE：','不小于10%'])

    #营业收入增长
    df1=df1[~(df1['yysr_g']<5)]    
    xgsm.append(['营业收入增长率：','不小于5%。这样设置旨在排除已公布财报，营业收入减少的股票。'])
    
    #上市时间
    df1=df1[(df1['ssrq']<'20150101')]
    xgsm.append(['上市日期：','2015年1月1日前上市'])
    
    #股价小于60
    df1=df1[(df1['gj']<60)]
    xgsm.append(['当前股价：','低于60元'])

    #2018业绩预测eps增长率>12%    
    df1=df1[(df1['eps1_g']>=10)]
    xgsm.append(['2018业绩预测：','eps增长率预测>10%'])
    
    #2017前3年复合增长率不小于5
    cols=zzl2.columns    
    df1=df1[~(df1[cols[0]]<5)]
    xgsm.append(['营业收入前3年复合增长率：','不小于5%'])
    
    #2017前3年复合增长率不小于5%    
    df1=df1[~(df1[cols[1]]<5)]
    xgsm.append(['净利润前3年复合增长率：','不小于5%'])

    #筛选2016年基本eps增长率>0,eps>0
    pool = stkpool_2016()
    #2016年关键财务指标
    df1 =df1[(df1.index.isin(pool))]
    
    xgsm.append(['2016年财务指标：',
        '2016年基本PES>0.05，基本EPS增长率>-5%，营业总收入增长率>-5%，营业收入增长率>-5%，'+
        '净利润增长率>-5%，净利润>0，净资产收益率>5%。'])

    df1['zf1y']=df1.index.map(lambda x:x[:6]+'_'+y1+'_'+nw).map(get_zf)
    df1['zf2y']=df1.index.map(lambda x:x[:6]+'_'+y2+'_'+nw).map(get_zf)
    df1['zf6m']=df1.index.map(lambda x:x[:6]+'_'+m6+'_'+nw).map(get_zf)
    df1['zf3m']=df1.index.map(lambda x:x[:6]+'_'+m3+'_'+nw).map(get_zf)
    df1['zf1m']=df1.index.map(lambda x:x[:6]+'_'+m1+'_'+nw).map(get_zf)
    
    df2=pd.DataFrame(xgsm,columns=['项目名称','项目参数'])

    #股东户数同比减少
    df3=df1[(df1['gdhstb']<15)]
    xgsm.append(['股东户数同比：','当前股东户数比2016年12月31日增长不超过15%'])

    #股东户数季度环比不大于
    df3=df3[~(df3['gdhshb']>15)]
    xgsm.append(['股东户数季度环比减少：','当前股东户数比2017年9月30日的增长不超过15%，也就是没有出现大幅增长。'])

    df4=pd.DataFrame(xgsm,columns=['项目名称','项目参数'])

    fn = r'd:\selestock\peg'+now+'.xlsx'

    if os.path.exists(fn):
        os.remove(fn)

    writer=pd.ExcelWriter(fn,engine='xlsxwriter')

    jbxx.to_excel(writer, sheet_name='基本信息')   
    df.to_excel(writer, sheet_name='业绩信息')   
    df1.to_excel(writer, sheet_name='选股一')  
    df2.to_excel(writer, sheet_name='选股一参数',index=False)  
    df3.to_excel(writer, sheet_name='选股二')  
    df4.to_excel(writer, sheet_name='选股二参数',index=False)  
    writer.save()

    
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
    xlsfn=getdrive()+'\\hyb\\pjjgmc.xlsx'
    wb = xw.Book(xlsfn)

    #读取数据
    data = wb.sheets[0].range('A1').options(pd.DataFrame, expand='table').value

    '''下面的语句很重要，MultiIndex转换成Index'''
    data.columns=[e[0] for e in data.columns]
    data['pjjg']=data.index
    
    xw.apps[0].quit()

    return data

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
             select gpdm,rq,pjjg,eps from gaorpt  
             where nf='2018' 
             union select distinct gpdm,rq,jg as pjjg,eps from cfi_yb 
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
def get_2018ylyc():
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    cn = sqlite3.connect(dbfn)
    curs = cn.cursor()
    rq = (datetime.datetime.now()-datetime.timedelta(183)).strftime('%Y-%m-%d')
#    rq = '2017-12-01'

    sql = '''select gpdm,rq,pjjg,eps,ly from (
             select gpdm,rq,pjjg,eps,"gao" as ly from gaorpt  
             where nf='2018' 
             union select distinct gpdm,rq,jg as pjjg,eps,"cfi" as ly from cfi_yb 
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
#
##########################################################################
def xg_peg3():
    now = datetime.datetime.now().strftime('%Y%m%d_%H%M')

    #获取股票基本信息
    jbxx=get_jbxx()
    
    #2017年业绩
    yj=get_yj2017()

    #营业收入和净利润3年复合增长率
    jbxx['gpdm']=jbxx.index
    
    #2017年3季度3年复合增长率
    nf_jd_y='2017_3_1' 
    zzl0=jbxx['gpdm'].map(lambda x:x+'|'+nf_jd_y).map(get_g)
    zzl0=zzl0.apply(lambda x: pd.Series([x[0],x[1]]))
    zzl0.columns=['yysr_g'+nf_jd_y,'jlr_g'+nf_jd_y]
    jbxx=jbxx.join(zzl0)

    nf_jd_y='2017_3_2' 
    zzl1=jbxx['gpdm'].map(lambda x:x+'|'+nf_jd_y).map(get_g)
    zzl1=zzl1.apply(lambda x: pd.Series([x[0],x[1]]))
    zzl1.columns=['yysr_g'+nf_jd_y,'jlr_g'+nf_jd_y]
    jbxx=jbxx.join(zzl1)

    nf_jd_y='2017_3_3' 
    zzl2=jbxx['gpdm'].map(lambda x:x+'|'+nf_jd_y).map(get_g)
    zzl2=zzl2.apply(lambda x: pd.Series([x[0],x[1]]))
    zzl2.columns=['yysr_g'+nf_jd_y,'jlr_g'+nf_jd_y]
    jbxx=jbxx.join(zzl2)

    nf_jd_y='2017_3_5' 
    zzl3=jbxx['gpdm'].map(lambda x:x+'|'+nf_jd_y).map(get_g)
    zzl3=zzl3.apply(lambda x: pd.Series([x[0],x[1]]))
    zzl3.columns=['yysr_g'+nf_jd_y,'jlr_g'+nf_jd_y]
    jbxx=jbxx.join(zzl3)

    nf_jd_y='2017_3_7' 
    zzl4=jbxx['gpdm'].map(lambda x:x+'|'+nf_jd_y).map(get_g)
    zzl4=zzl4.apply(lambda x: pd.Series([x[0],x[1]]))
    zzl4.columns=['yysr_g'+nf_jd_y,'jlr_g'+nf_jd_y]
    jbxx=jbxx.join(zzl4)

    nf_jd_y='2017_3_8' 
    zzl5=jbxx['gpdm'].map(lambda x:x+'|'+nf_jd_y).map(get_g)
    zzl5=zzl5.apply(lambda x: pd.Series([x[0],x[1]]))
    zzl5.columns=['yysr_g'+nf_jd_y,'jlr_g'+nf_jd_y]
    jbxx=jbxx.join(zzl5)

    jbxx=jbxx.drop('gpdm',axis=1)

    
    #只提取有业绩信息的股票
    jbxx1=jbxx[jbxx.index.isin(yj.index)]
    df=jbxx1.join(yj)
    
    #计算PE
    df=df.assign(pe = df['gj']/df['eps'])
    #eps小于等于0,PE无意义
    df.loc[df['eps']<=0,'pe']=np.nan
    #股价为0,停牌
    df.loc[df['gj']<=0,'pe']=np.nan
    
    #计算PEG
    df=df.assign(peg = df['pe']/df['jlr_g'])
 
    #净利润同比增长率为负的，PEG也没有意义
    df.loc[df['jlr_g']<=0,'peg']=np.nan
    
    #上一年净利润为负，净利润同比增长率也没有意义
    df.loc[df['jlr_1']<0,'peg']=np.nan

    #计算pb
    df=df.assign(pb = df['gj']/df['mgjzc'])
    df.loc[df['mgjzc']<=0,'pb']=np.nan
    df.loc[df['gj']<=0,'pb']=np.nan

    #盈利预测
    ylyc=get_ylyc2018()
    df=df.join(ylyc)
    
    df=df.assign(eps1_g = (df['mean']/df['eps']-1)*100)
    df=df.assign(eps1_g_x = (df['amax']/df['eps']-1)*100)
    df=df.assign(eps1_g_n = (df['amin']/df['eps']-1)*100)

    #未来一年预测pe1、peg1
    df=df.assign(pe1 = df['gj']/df['mean'])
    df=df.assign(peg1 = df['pe1']/df['eps1_g'])

    df=df.round(2)


    #选股说明
    xgsm={}
    xgsm['选股时间']=now
    #选股

    #上市时间
    df0=df[(df['ssrq']<'20150101')]
    xgsm['上市日期']='2015年1月1日前上市'
    
    #PE小于30
    df0=df0[(df0['pe']<50)]
    xgsm['市盈率']='小于50'
    
    #净利润增长率大于12%
    df0=df0[(df0['jlr_g']>12)]
    xgsm['净利润增长率']='在12%以上'

    #PEG小于1
    df0=df0[(df0['peg']<1.5)]
    xgsm['PEG']='小于1.5'

    #筛选2016年基本eps增长率>0,eps>0
    pool = stkpool_2016()
    #2016年关键财务指标
    df0 =df0[(df0.index.isin(pool))]
    
    xgsm['2016年财务指标']='2016年基本PES>0.05，基本EPS增长率>-5%，营业总收入增长率>-15%，营业收入增长率>-15%，净利润增长率>-5%，净利润>0，净资产收益率>0%。'

    td=datetime.datetime.now()
    nw=td.strftime("%Y%m%d")
    y1=(td+datetime.timedelta(-365)).strftime("%Y%m%d")
    y2=(td+datetime.timedelta(-365*2)).strftime("%Y%m%d")
    m6=(td+datetime.timedelta(-30*6)).strftime("%Y%m%d")
    m3=(td+datetime.timedelta(-30*3)).strftime("%Y%m%d")
    m1=(td+datetime.timedelta(-30*1)).strftime("%Y%m%d")
    
    df0['zf1y']=df0.index.map(lambda x:x[:6]+'_'+y1+'_'+nw).map(get_zf)
    df0['zf2y']=df0.index.map(lambda x:x[:6]+'_'+y2+'_'+nw).map(get_zf)
    df0['zf6m']=df0.index.map(lambda x:x[:6]+'_'+m6+'_'+nw).map(get_zf)
    df0['zf3m']=df0.index.map(lambda x:x[:6]+'_'+m3+'_'+nw).map(get_zf)
    df0['zf1m']=df0.index.map(lambda x:x[:6]+'_'+m1+'_'+nw).map(get_zf)

    xgsm1=xgsm.copy()

    #ROE大于10
    df1=df0[~(df0['roe']<10)]
    xgsm['ROE']='不小于10%'

    #营业收入增长
    df1=df1[~(df1['yysr_g']<5)]    
    xgsm['营业收入增长率']='不小于5%。这样设置旨在排除已公布财报，营业收入减少的股票。'
    
    #股价小于60
    df1=df1[(df1['gj']<60)]
    xgsm['当前股价']='低于60元'

    #2017前3年复合增长率不小于5
    cols=zzl2.columns    
    df1=df1[~(df1[cols[0]]<5)]
    xgsm['营业收入前3年复合增长率']='不小于5%'
    
    #2017前3年复合增长率不小于5%    
    df1=df1[~(df1[cols[1]]<5)]
    xgsm['净利润前3年复合增长率']='不小于5%'
    
    #2018业绩预测eps增长率>15%    
    df1=df1[(df1['eps1_g']>=10)]
    xgsm['2018业绩预测来源']='2017年12月01日以来的机构预测报告（取最新值）'
    xgsm['2018业绩预测']='eps增长率预测>10%'
    
    xgsm2=xgsm.copy()
    
    ser1=pd.Series(xgsm)
    ser1.index.name='项目名称'
    ser1.name='项目参数'

    #股东户数同比减少
    df3=df1[(df1['gdhstb']<15)]
    xgsm['股东户数同比']='当前股东户数比2016年12月31日增长不超过15%'

    #股东户数季度环比不大于
    df3=df3[~(df3['gdhshb']>15)]
    xgsm['股东户数季度环比减少']='当前股东户数比2017年9月30日的增长不超过15%，也就是没有出现大幅增长。'

    df3=df3[(df3['pe']<30)]
    xgsm['市盈率']='小于30'

    df3=df3[(df3['peg']<1.2)]
    xgsm['PEG']='小于1.2'

    ser2=pd.Series(xgsm)
    ser2.index.name='项目名称'
    ser2.name='项目参数'


    df2=df0[(df0['pe']<30)]
    xgsm1['市盈率']='小于30'

    #净利润增长率大于12%
    df2=df2[(df2['jlr_g']>15)]
    xgsm1['净利润增长率']='在15%以上'
 
    #PEG小于1
    df2=df2[(df2['peg']<1.2)]
    xgsm1['PEG']='小于1.2'

    ser3=pd.Series(xgsm1)
    ser3.index.name='项目名称'
    ser3.name='项目参数'

    #选股四
    df4=df1[(df1['eps1_g']>=15)]
    xgsm2['2018业绩预测']='eps增长率预测>15%'

    #股东户数同比减少
    df4=df4[(df4['gdhstb']<10)]
    xgsm2['股东户数同比']='当前股东户数比2016年12月31日增长不超过10%'

    #股东户数季度环比不大于
    df4=df4[~(df4['gdhshb']>10)]
    xgsm2['股东户数季度环比减少']='当前股东户数比2017年9月30日的增长不超过10%，也就是没有出现大幅增长。'

    df4=df4[(df4['pe1']<30)]
    xgsm2['2018年市盈率预测值']='小于30'

    df4=df4[(df4['peg1']<1.2)]
    xgsm2['2018年PEG预测值']='小于1.2'

    ser4=pd.Series(xgsm2)
    ser4.index.name='项目名称'
    ser4.name='项目参数'


    fn = r'd:\selestock\peg'+now+'.xlsx'

    if os.path.exists(fn):
        os.remove(fn)

    writer=pd.ExcelWriter(fn,engine='xlsxwriter')

    jbxx.to_excel(writer, sheet_name='基本信息')   
    df.to_excel(writer, sheet_name='业绩信息')   
    df1.to_excel(writer, sheet_name='选股一')  
    ser1.to_excel(writer, sheet_name='选股一参数')  
    df3.to_excel(writer, sheet_name='选股二')  
    ser2.to_excel(writer, sheet_name='选股二参数')  
    df2.to_excel(writer, sheet_name='选股三')  
    ser3.to_excel(writer, sheet_name='选股三参数')  
    df4.to_excel(writer, sheet_name='选股四')  
    ser4.to_excel(writer, sheet_name='选股四参数')  
    writer.save()

##########################################################################
#
##########################################################################
def xg_peg4():
    now = datetime.datetime.now().strftime('%Y%m%d_%H%M')

    #获取股票基本信息
    jbxx=get_jbxx()
    
    #2017年业绩
    yj=get_yj2017()

    #营业收入和净利润3年复合增长率
    
    #2018年1季度3年复合增长率
    nf_jd_y='2018_1_1' 
    zzl0=jbxx['gpdm'].map(lambda x:x+'|'+nf_jd_y).map(get_g)
    zzl0=zzl0.apply(lambda x: pd.Series([x[0],x[1]]))
    zzl0.columns=['yysr_g'+nf_jd_y,'jlr_g'+nf_jd_y]
    jbxx=jbxx.join(zzl0)

    nf_jd_y='2018_1_2' 
    zzl1=jbxx['gpdm'].map(lambda x:x+'|'+nf_jd_y).map(get_g)
    zzl1=zzl1.apply(lambda x: pd.Series([x[0],x[1]]))
    zzl1.columns=['yysr_g'+nf_jd_y,'jlr_g'+nf_jd_y]
    jbxx=jbxx.join(zzl1)

    nf_jd_y='2018_1_3' 
    zzl2=jbxx['gpdm'].map(lambda x:x+'|'+nf_jd_y).map(get_g)
    zzl2=zzl2.apply(lambda x: pd.Series([x[0],x[1]]))
    zzl2.columns=['yysr_g'+nf_jd_y,'jlr_g'+nf_jd_y]
    jbxx=jbxx.join(zzl2)

    nf_jd_y='2018_1_5' 
    zzl3=jbxx['gpdm'].map(lambda x:x+'|'+nf_jd_y).map(get_g)
    zzl3=zzl3.apply(lambda x: pd.Series([x[0],x[1]]))
    zzl3.columns=['yysr_g'+nf_jd_y,'jlr_g'+nf_jd_y]
    jbxx=jbxx.join(zzl3)

    nf_jd_y='2018_1_7' 
    zzl4=jbxx['gpdm'].map(lambda x:x+'|'+nf_jd_y).map(get_g)
    zzl4=zzl4.apply(lambda x: pd.Series([x[0],x[1]]))
    zzl4.columns=['yysr_g'+nf_jd_y,'jlr_g'+nf_jd_y]
    jbxx=jbxx.join(zzl4)


    
    #只提取有业绩信息的股票
    jbxx1=jbxx[jbxx.index.isin(yj.index)]
    df=jbxx1.join(yj)

    #5月1日后用上年年报eps
    df['eps']=df['eps2017']
    df['jlr_g']=df['eps_g2017']
    
    
    #计算PE
    df=df.assign(pe = df['gj']/df['eps'])
    #eps小于等于0,PE无意义
    df.loc[df['eps']<=0,'pe']=np.nan
    #股价为0,停牌
    df.loc[df['gj']<=0,'pe']=np.nan
    
    #计算PEG
    df=df.assign(peg = df['pe']/df['jlr_g'])
 
    #净利润同比增长率为负的，PEG也没有意义
    df.loc[df['jlr_g']<=0,'peg']=np.nan
    
    #上一年净利润为负，净利润同比增长率也没有意义
    df.loc[df['jlr_1']<0,'peg']=np.nan

    #计算pb
    df=df.assign(pb = df['gj']/df['mgjzc'])
    df.loc[df['mgjzc']<=0,'pb']=np.nan
    df.loc[df['gj']<=0,'pb']=np.nan

    #盈利预测
    #港澳资讯的相对准确，优先使用，大智慧、东方财富的作为补充
#    ylyc1=get_ylyc2018_gao()
#    ylyc2=get_ylyc2018_dzh()
#    ylyc3=get_ylyc2018()
#    ylyc=pd.concat([ylyc1,ylyc2,ylyc3])
#    ylyc=ylyc[~ylyc.index.duplicated(keep='first')]
    
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

    ser=pd.Series(xgsm)
    ser.index.name='项目名称'
    ser.name='项目参数'
    
#    td=datetime.datetime.now()
#    nw=td.strftime("%Y%m%d")
#    y1=(td+datetime.timedelta(-365)).strftime("%Y%m%d")
#    y2=(td+datetime.timedelta(-365*2)).strftime("%Y%m%d")
#    m6=(td+datetime.timedelta(-30*6)).strftime("%Y%m%d")
#    m3=(td+datetime.timedelta(-30*3)).strftime("%Y%m%d")
#    m1=(td+datetime.timedelta(-30*1)).strftime("%Y%m%d")
#    
#    df1['zf1y']=df1.index.map(lambda x:x[:6]+'_'+y1+'_'+nw).map(get_zf)
#    df1['zf2y']=df1.index.map(lambda x:x[:6]+'_'+y2+'_'+nw).map(get_zf)
#    df1['zf6m']=df1.index.map(lambda x:x[:6]+'_'+m6+'_'+nw).map(get_zf)
#    df1['zf3m']=df1.index.map(lambda x:x[:6]+'_'+m3+'_'+nw).map(get_zf)
#    df1['zf1m']=df1.index.map(lambda x:x[:6]+'_'+m1+'_'+nw).map(get_zf)
    

    zfdf=df1['dm'].map(zf_calc)
    
    #Python pandas 数据框的str列内置的方法详解
    #https://blog.csdn.net/qq_28219759/article/details/52919233
    df2=df1[~(df1['zyts'].str.contains('解禁'))]

    fn = r'd:\selestock\peg2018_'+now+'.xlsx'

    if os.path.exists(fn):
        os.remove(fn)

    jbxx=jbxx.drop(columns=['gpdm','dm'])
    df=df.drop(columns=['gpdm','dm'])
    df1=df1.drop(columns=['gpdm','dm'])
    df2=df2.drop(columns=['gpdm','dm'])

    writer=pd.ExcelWriter(fn,engine='xlsxwriter')

    jbxx.to_excel(writer, sheet_name='基本信息')   
    df.to_excel(writer, sheet_name='业绩信息')   
    df1.to_excel(writer, sheet_name='选股一')  
    ser.to_excel(writer, sheet_name='选股一参数')  
    df2.to_excel(writer, sheet_name='选股一(去掉半年内有解禁)')  

    writer.save()


##########################################################################
#涨幅统计
##########################################################################
def calc_zf():
    
    df=get_jbxx()
    td=datetime.datetime.now()
    nw=td.strftime("%Y%m%d")
    y10=(td+datetime.timedelta(-365*10)).strftime("%Y%m%d")
    y5=(td+datetime.timedelta(-365*5)).strftime("%Y%m%d")
    y3=(td+datetime.timedelta(-365*3)).strftime("%Y%m%d")
    m3=(td+datetime.timedelta(-30*3)).strftime("%Y%m%d")
    
##    y1=(td+datetime.timedelta(-365)).strftime("%Y%m%d")
##    y2=(td+datetime.timedelta(-365*2)).strftime("%Y%m%d")
##    m6=(td+datetime.timedelta(-30*6)).strftime("%Y%m%d")
##    m3=(td+datetime.timedelta(-30*3)).strftime("%Y%m%d")
##    m1=(td+datetime.timedelta(-30*1)).strftime("%Y%m%d")
#    
#    
    df['zf10y']=df.index.map(lambda x:x[:6]+'_'+y10+'_'+nw).map(get_zf1)
    df['zf5y']=df.index.map(lambda x:x[:6]+'_'+y5+'_'+nw).map(get_zf1)
    df['zf3y']=df.index.map(lambda x:x[:6]+'_'+y3+'_'+nw).map(get_zf1)
    df['zf3m']=df.index.map(lambda x:x[:6]+'_'+m3+'_'+nw).map(get_zf1)
    
##    df['zf1y']=df.index.map(lambda x:x[:6]+'_'+y1+'_'+nw).map(get_zf)
##    df['zf2y']=df.index.map(lambda x:x[:6]+'_'+y2+'_'+nw).map(get_zf)
##    df['zf6m']=df.index.map(lambda x:x[:6]+'_'+m6+'_'+nw).map(get_zf)
##    df['zf3m']=df.index.map(lambda x:x[:6]+'_'+m3+'_'+nw).map(get_zf)
##    df['zf1m']=df.index.map(lambda x:x[:6]+'_'+m1+'_'+nw).map(get_zf)
#
    df.to_excel(r'd:\selestock\zf3.xlsx') 

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

    cols = ['gpdm','eps0','eps0_g']
    
    df=pd.DataFrame(data,columns=cols)
    df=df.set_index('gpdm')
    df=df[df.index.isin(ssrq.index)]
    
    return df

##########################################################################
#交易分析
##########################################################################
def jyfx(gpdm,kzj):
    
    mrj=np.nan
    htj=np.nan
    zgj=np.nan
    zdj=np.nan
    
    mrbj=False
    
    htrq=None
    mrrq=None
    zgrq=None
    zdrq=None
    
    htts=None
    zgts=None
    zdts=None
    
    k=get_k_data(gpdm,'20170301','20180322','hfq')
    for i in range(len(k)):
        rq=k.iloc[i].name
        
        #买入操作
        if k.iloc[i]['low']<kzj and not mrbj:
            mrrq=rq
            mrbj=True
            mrj=k.iloc[i]['close']
            
            #如果不是最后一天，则最高、回调、最低均从次日算起
            if i<len(k)-1:
                
                zgj=k.iloc[i+1]['high']
                zdj=k.iloc[i+1]['low']
                htj=k.iloc[i+1]['low']
                
                zgrq=k.iloc[i+1].name
                zdrq=zgrq
                htrq=zgrq
                htts=1
                zgts=1
                zdts=1

                j=i
                
        #最高价、最低价    
        if mrbj and rq>mrrq :
            if k.iloc[i]['high']>zgj:
                zgj=k.iloc[i]['high']
                zgrq=rq
                zgts=i-j
                
            if k.iloc[i]['low']<zdj:
                zdj=k.iloc[i]['low']
                zdrq=rq
                zdts=i-j
    
    #最高价前的回调价            
    if mrrq!=None and zgrq!=None :    
        k=k[mrrq:zgrq] 

        for i in range(1,len(k)):
            if k.iloc[i]['low']<htj:
                htj=k.iloc[i]['low']
                htrq=k.iloc[i].name
                htts=i
                
    #买入需在2017年7月1日前有效
    if mrrq!=None and mrrq<'20170701':
        rt=[gpdm,mrrq,mrj,htrq,htj,zgrq,zgj,zdrq,zdj,(htj/mrj-1)*100,(zgj/mrj-1)*100,(zdj/mrj-1)*100,htts,zgts,zdts]
    else:    
        rt=[gpdm,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
    
    return rt


##########################################################################
#持股分析
##########################################################################
def cgfx(gpdm,kzj,nf,eps,eps_g):
    cgts=180    #持股天数(自然天数)
    bcdf=-10    #补仓跌幅
    zsdf=-15    #止损跌幅
    zyzf=20     #止赢涨幅
    
    mrj=np.nan      #买入价
    mrpe=np.nan     #买入pe
    mrpeg=np.nan    #买入peg
    zgj=np.nan      #最高价
    zdj=np.nan      #最低价
    bcj1=np.nan      #补仓价1

    bcj2=np.nan      #补仓价2

    htj1=np.nan      #回调价1：最后买入到卖出期间最低价
    htj2=np.nan      #回调价2：最后买入到后面最高价期间的最低价
    zsj=np.nan      #止损价
    zyj=np.nan      #止赢价
    mcj=np.nan      #卖出价
    cbj=np.nan      #成本价
        
    jyzt=0          #交易状态：0还未买入，1已买入未补仓，2已买入1次补仓，3已买入2次补仓，4已卖出

    
    #出现日期
    mrrq=None      #买入日期
    bcrq1=None      #补仓日期
    bcrq2=None      #补仓日期
    htrq1=None      #回调日期1:最后买入到卖出
    htrq2=None      #回调日期2：最后买入到最高价
    zgrq=None      #最高日期
    zdrq=None      #最低日期
    mcrq=None      #卖出日期
    dqrq=None      #到期日期

    #距离买入日期天数
    zgts=None      #最高价天数
    zdts=None      #最低价天数
    ccts=None      #持仓天数
    
    mclx=None      #卖出类型：
    syl=np.nan     #收益率%
    
    d1=str(nf)+'0301'
    d2=str(nf+1)+'0401'
    d3=str(nf)+'0701'
    k=get_k_data(gpdm,d1,d2,'hfq')
    
    #提取年报至
    k1=k[d1:d3]
    #停牌或无买入点
    if len(k1)==0 or k1['low'].min()>kzj :
        rt=[gpdm,None,None,None,None,None,None,None,None,None,None,None,None,None,
            None,None,None,None,None,None,None,None,None,None,None,None,None,None,
            None,None,None,None,None,None,None,None,None]
        return rt
        
    for i in range(len(k)):
        rq=k.iloc[i].name

        
        '''买入操作
        
        '''
        if k.iloc[i]['low']<kzj and jyzt==0:
            mrrq=rq
            dqrq=nextdtstr(mrrq,cgts)   
            jyzt=1
            mrj=k.iloc[i]['close']
            cbj=mrj
            bcj1=round(cbj*(1+bcdf/100),2)
            bcj2=round(cbj*(1+2*bcdf/100),2)
            zyj=round(cbj*(1+zyzf/100),2)
            zsj=round(cbj*(1+zsdf/100),2)
            
            mrpe=mrj/eps
            mrpeg=mrpe/eps_g
            
            #如果不是最后一天，则最高、回调、最低均从次日算起
            if i<len(k)-1:
                zgj=k.iloc[i+1]['high']
                zdj=k.iloc[i+1]['low']
                zgrq=k.iloc[i+1].name
                zdrq=zgrq
                zgts=1
                zdts=1

            j=i     #记录买入位置
                
        '''记录买入以后的最高、最低价操作       
        '''
        if jyzt>0 and rq>mrrq :
            
            if k.iloc[i]['high']>zgj:
                zgj=k.iloc[i]['high']
                zgrq=rq
                zgts=i-j
                
            if k.iloc[i]['low']<zdj:
                zdj=k.iloc[i]['low']
                zdrq=rq
                zdts=i-j
            
        '''到期操作
        条件：已买入、日期大于等于到期日期
        处理：记录卖出日期和卖出价(收盘价)
        '''
        if jyzt in (1,2,3) and rq>=dqrq:
            mcrq=rq
            mcj=k.iloc[i]['close']
            jyzt=4
            ccts=i-j
            mclx='到期'

        '''止赢操作
        条件：已买入、最高价大于止盈价
        处理：记录卖出日期和卖出价
        '''
        if jyzt in (1,2,3) and k.iloc[i]['high']>zyj:
            mcrq=rq
            mcj=zyj
            jyzt=4
            ccts=i-j
            mclx='止赢'
            
        '''止损操作
        条件：已买入、已补仓、最低价小于止盈价
        处理：记录卖出日期和卖出价
        '''
        if jyzt==3 and k.iloc[i]['low']<zsj:
            mcrq=rq
            mcj=zsj
            jyzt=4
            ccts=i-j
            mclx='止损'
            
        '''补仓操作
        条件：已经买入、允许补仓、未卖出、当日最低价小于补仓价
        处理:计算买入价(成本价)、止赢价、止损价，记录补仓日期
        补仓后需重新记录最高、最低价
        '''
        if jyzt==1 and k.iloc[i]['low']<bcj1:
            cbj=(mrj+bcj1*2)/3   #股份加倍补仓法
            zyj=round(cbj*(1+zyzf/100),2)
            zsj=round(cbj*(1+zsdf/100),2)
            bcrq1=rq
            jyzt=2
            
            if i<len(k)-1:
                zgj=k.iloc[i+1]['high']
                zdj=k.iloc[i+1]['low']
                zgrq=k.iloc[i+1].name
                zdrq=zgrq
                zgts=i-j+1
                zdts=i-j+1
        
        #第2次补仓
        if jyzt==2 and k.iloc[i]['low']<bcj2:
            cbj=(cbj+bcj2)/2   #股份等份补仓法
            zyj=round(cbj*(1+zyzf/100),2)
            zsj=round(cbj*(1+zsdf/100),2)
            bcrq2=rq
            jyzt=3
            
            if i<len(k)-1:
                zgj=k.iloc[i+1]['high']
                zdj=k.iloc[i+1]['low']
                zgrq=k.iloc[i+1].name
                zdrq=zgrq
                zgts=i-j+1
                zdts=i-j+1
                
    #最后买入日期
    last=bcrq2 if bcrq2!=None else (bcrq1 if bcrq1!=None else mrrq)
    #持股到卖出回调
    k1=k[last:mcrq]
    htj1=k1['low'].min()
    htrq1=k1['low'].idxmin()
    htdf1=(htj1-cbj)/cbj*100    
    ht1pe=htj1/eps
    ht1peg=ht1pe/eps_g

    #持股到最高价回调
    k1=k[last:zgrq]
    htj2=k1['low'].min()
    htrq2=k1['low'].idxmin()
    htdf2=(htj2-cbj)/cbj*100    
    ht2pe=htj2/eps
    ht2peg=ht2pe/eps_g

    #收益率        
    syl=(mcj-cbj)/cbj*100
    
    #一直持股
    zgsyl=(zgj-cbj)/cbj*100
    zdsyl=(zdj-cbj)/cbj*100
    zgpe=zgj/eps
    zgpeg=zgpe/eps_g
    zdpe=zdj/eps
    zdpeg=zdpe/eps_g
    
    
    rt=[gpdm,mrrq,mrj,mrpe,mrpeg,bcrq1,bcj1,bcrq2,bcj2,mcrq,mcj,mclx,cbj,syl,ccts,zgrq,zgj,zgpe,zgpeg,zgsyl,zgts,
        zdrq,zdj,zdpe,zdpeg,zdsyl,zdts,htrq1,htj1,ht1pe,ht1peg,htdf1,htrq2,htj2,ht2pe,ht2peg,htdf2]

    return rt


##########################################################################
#2017年3-6月peg选股验证
##########################################################################
def peg_test_2017():
    gpssrq=get_ssrq()
    #股票代码、名称、拼音
    gpdmb=get_gpdm()
    #股票申万行业
    gpswhy=get_swhy()
    
    
    #最近4年roe
    gproe=roe_nf_n(2016,4)
    
    #最近4年eps0
    gpeps=eps_nf_n(2016,4)

    #最近4年eps0_g
    gpepsg=epsg_nf_n(2016,4)
    #信息合并
    jbxx=gpdmb.join(gpssrq)
    jbxx=jbxx.join(gpswhy)
    jbxx=jbxx.join(gproe)
    jbxx=jbxx.join(gpeps)
    jbxx=jbxx.join(gpepsg)

    gpc=pool_peg(2017)
    #按增长率g%，peg=1计算控制价，即pe=g，kzj=eps*g当股价小于该价位时买入
    g=30
    gpc['g']=g
    gpc.loc[gpc['eps0_g']<g,'g']=gpc['eps0_g']
    gpc.eval('kzj = eps0*g',inplace=True)

    jbxx=jbxx[jbxx.index.isin(gpc.index)]
    gpc=jbxx.join(gpc)
    
    mrjs=gpc['kzj']
    jyjg=[]
    
    d=50    #行长度
    
    for i in range(len(mrjs)):
        
        ln='%s\n'%(str(i+1),) if (int((i+1)/d)==(i+1)/d or i==len(mrjs)-1) else '' 
        print('#',end=ln)
        jyjg.append(jyfx(mrjs.index[i],mrjs[i]))
        
    df=pd.DataFrame(jyjg,columns=['gpdm','mrrq','mrj','htrq','htj','zgrq','zgj','zdrq','zdj','htdf','zdzf','zddf','htts','zgts','zdts'])
    df=df.set_index('gpdm')
    df=gpc.join(df)
    df=df.round(4)
    df.to_excel(r'd:\selestock\jyfx2017_'+str(g)+'.xlsx')

##########################################################################
#持股分析
##########################################################################
def peg_cgfx(nf):
    gpssrq=get_ssrq()
    #股票代码、名称、拼音
    gpdmb=get_gpdm()
    #股票申万行业
    gpswhy=get_swhy()
    
    
    #最近4年roe
    gproe=roe_nf_n(nf-1,4)
    
    #最近4年eps0
    gpeps=eps_nf_n(nf-1,4)

    #最近4年eps0_g
    gpepsg=epsg_nf_n(nf-1,4)
    #信息合并
    jbxx=gpdmb.join(gpssrq)
    jbxx=jbxx.join(gpswhy)
    jbxx=jbxx.join(gproe)
    jbxx=jbxx.join(gpeps)
    jbxx=jbxx.join(gpepsg)

    gpc=pool_peg(nf)
    
    #按增长率g%，peg=1计算控制价，即pe=g，kzj=eps*g当股价小于该价位时买入
    g=40
    gpc['g']=g
    gpc.loc[gpc['eps0_g']<g,'g']=gpc['eps0_g']
    gpc.eval('kzj = eps0*g',inplace=True)

    jbxx=jbxx[jbxx.index.isin(gpc.index)]
    gpc=jbxx.join(gpc)
    
    
    jyjg=[]
    
    d=50    #行长度
    gpsl=len(gpc)
    for i in range(gpsl):
        
        ln='%s/%s\n'%(str(i+1),str(gpsl)) if (int((i+1)/d)==(i+1)/d or i==gpsl-1) else '' 
        print('#',end=ln)

        gpdm=gpc.iloc[i].name
        kzj=gpc.iloc[i]['kzj']
        eps=gpc.iloc[i]['eps0']
        eps_g=gpc.iloc[i]['eps0_g']
              
        jyjg.append(cgfx(gpdm,kzj,nf,eps,eps_g))
        
    df=pd.DataFrame(jyjg,columns=['gpdm','mrrq','mrj','mrpe','mrpeg','bcrq1','bcj1','bcrq2','bcj2',
                                  'mcrq','mcj','mclx','cbj','syl','ccts',
                                  'zgrq','zgj','zgpe','zgpeg','zgsyl','zgts','zdrq','zdj','zdpe','zdpeg','zdsyl','zdts',
                                  'htrq1','htj1','ht1pe','ht1peg','htdf1','htrq2','htj2','ht2pe','ht2peg','htdf2'])
    df=df.set_index('gpdm')
    df=gpc.join(df)
    df=df.round(4)
    df.to_excel(r'd:\selestock\cgfx'+str(nf)+'_'+str(g)+'.xlsx')
 
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

##########################################################################
#根据自由现金流含量、资产报酬率、净利率选取的好股票
##########################################################################
def good():
    gpssrq=get_ssrq()
    #股票代码、名称、拼音
    gpdmb=get_gpdm()
    #股票申万行业
    gpswhy=get_swhy()
    
    jbxx=gpdmb.join(gpssrq)
    jbxx=jbxx.join(gpswhy)
    
    nf1=2014
    nf2=2017
    df1 = get_srzyxjhl1(nf1,nf2)
    df2 = get_srzyxjhl2(nf1,nf2,4)
    df3 = get_roe(nf1,nf2,12)
    df4 = get_roa(nf1,nf2,5)
    df5 = get_jll(nf1,nf2,12)
    
    df=df1.join(df2,how='inner')
    df=df.join(df3,how='inner')
    df=df.join(df4,how='inner')
    df=df.join(df5,how='inner')

    df=jbxx.join(df,how='inner')
    
    df.to_excel(r'd:\selestock\good.xlsx')

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
    stk=[e[0][:6] for e in data]
    stnum=len(stk)
    blkname='股份回购'
    if stnum>0:
        blk[blkname] = [blkname,stnum,stk]

    return blk


    
###############################################################################
#高管股东持股变动信息
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

    return blk


###############################################################################
#限售解禁信息
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
#提取股票所属通达信概念、风格、指数中重要的概念、风格与指数形成pd
###############################################################################
def get_zyts():
    gfz=tdxgnfgzs()
    xsjj=xsjjxx()
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
#程序路径
##########################################################################
def samepath():
    (filepath,tempfilename) = os.path.split(sys.argv[0])
    return filepath

##########################################################################
#最新股价
##########################################################################
def get_todaygj():
    
    csvfn = os.path.join(samepath(),'today_gj.csv')

        
    dlyes = False    #下载标志，True表示要下载

    if os.path.exists(csvfn):
        
        mtime=os.path.getmtime(csvfn)  #文件建立时间
        ltime=time.strftime("%Y%m%d",time.localtime(mtime))
        if ltime >= lastday() :
            #必须是最后一个交易日16点以后下载的最新数据
            if ltime == lastday() and time.strftime("%H",time.localtime(mtime))<'16':
                dlyes = True
            else:    
                dlyes = False
        else :
            dlyes = True
    else :
        dlyes = True


    if dlyes:    
        
        gpdqgj=ts.get_today_all()
        print('\n')
        #生成gpdm修改索引
        gpdqgj['gpdm']=gpdqgj['code'].map(lambda x:x+('.SH' if x[0]=='6' else '.SZ'))
        #提取两列
        gpdqgj=gpdqgj[['gpdm','trade']]
        #修改列名
        gpdqgj=gpdqgj.rename(columns={'trade': 'gj'})
        
        gpdqgj=gpdqgj.set_index('gpdm')
        
        #删除重复的数据
        gpdqgj=gpdqgj[~gpdqgj.index.duplicated(keep='first')]
        
        gpdqgj.loc[gpdqgj['gj']==0,'gj']=None
        
        gpdqgj.to_csv(csvfn)

    else:
        
        gpdqgj=pd.read_csv(csvfn)
        gpdqgj=gpdqgj.set_index('gpdm')

    return gpdqgj        
        
###############################################################################
#将通达信.day读入pands
###############################################################################
def tdxday2pd(gpdm,start=None,end=None):
    
    gpdm=sgpdm(gpdm)

    sc = 'sh' if gpdm[0]=='6' else 'sz'
#    dayfn =getdisk()+'\\tdx\\'+sc+'lday\\'+sc+gpdm+'.day'
    dayfn =gettdxdir()+'\\vipdoc\\'+sc+'\\lday\\'+sc+gpdm+'.day'

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

    #前复权
    df=df/a

    return df

###############################################################################
#前复权复权，复权因子是从tushare下载的csv文件
###############################################################################
def get_fqgj(gpdm):

    data=tdxday2pd(gpdm)
        
    #计算前复权因子
    adjf=factor_adj(gpdm)
    
    #前复权收盘价
    data=data.join(adjf)
    data=data.eval('adj_close = adj_close * adj_factor')

    fqgj=data[['date','adj_close']]
    '''
    pandas 将“字符类型的日期列”转化成“时间戳索引（DatetimeIndex）”
    https://www.jianshu.com/p/4ece5843d383
    '''
    fqgj=fqgj.set_index('date')    

    fqgj.index = pd.DatetimeIndex(fqgj.index)
    
    return fqgj.round(3)

###############################################################################
#查询dataframe,返回series
###############################################################################
def get_nearest(df,idx):
    idx=pd.to_datetime(idx)
    df=df.sort_index()
    if idx in df.index:
        return df.loc[idx]
    else:
        for i in range(len(df.index)):
            if idx>df.index[i]:
                continue
            else:
                return df.iloc[i]
            
        return df.iloc[i]
    
    
###############################################################################
#一次性计算多个区间涨幅
###############################################################################
def zf_calc(gpdm):
    print(gpdm)
    td=datetime.datetime.now()
    nw=td.strftime("%Y%m%d")

    y1=(td+datetime.timedelta(-365)).strftime("%Y%m%d")
    y2=(td+datetime.timedelta(-365*2)).strftime("%Y%m%d")
    m6=(td+datetime.timedelta(-30*6)).strftime("%Y%m%d")
    m3=(td+datetime.timedelta(-30*3)).strftime("%Y%m%d")
    m1=(td+datetime.timedelta(-30*1)).strftime("%Y%m%d")


    df=get_fqgj(gpdm)
    gj0=get_nearest(df,nw)['adj_close']
    zf=[]
    for dt in (y1,y2,m6,m3,m1):
        gj=get_nearest(df,dt)['adj_close']
        zf.append((gj/gj0-1)*100)
        
    return zf

    
if __name__ == '__main__':
#def sele_stk():
    print('%s Running' % sys.argv[0])
    now1 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now1)

#    xg_peg()

    start_time = time.time()
#    dlday()

#    dqgj=get_todaygj()

    now2 = datetime.datetime.now().strftime('%H:%M:%S')
    print('结束运行时间：%s' % now2)
    
#    xg_peg3()

#    xg_peg4()

#    df=ylyc2018()
#    peg_test_2017()

#    peg_cgfx(2017)

#    peg_cgfx(2016)
    
#    df=get_ylyc2018_dzh()
    
#    date='20180209'
#    gj=get_all_adjclose(date)

#   good()    
    
    now2 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now1)
    print('结束运行时间：%s' % now2)

