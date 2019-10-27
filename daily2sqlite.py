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
def day2df(dayfn,recent_days=0):
    

    columns = ['trade_date','open', 'high', 'low','close','amount','vol','pct_chg','pre_close']

    with open(dayfn,"rb") as f:
        data = f.read()
        f.close()
    
    days = int(len(data)/32)
    if recent_days==0:
        j = 0
    else :
        j = days - recent_days
        if j<0 :
            j=0
    
    records = []
    qsp = np.nan
    
    for i in range(j,days):
        dat = data[i*32:(i+1)*32]
        rq,kp,zg,zd,sp,cje,cjl,tmp = struct.unpack("iiiiifii", dat)

        rq = str(rq)
        kp = kp/100.00
        zg = zg/100.00
        zd = zd/100.00
        sp = sp/100.00
        cje = cje/100000000.00     #亿元
        cjl = cjl/10000.00         #万股
        zf = sp/qsp-1 if (i>0 and qsp>0) else np.nan

        records.append([rq,kp,zg,zd,sp,cje,cjl,zf,qsp])
        qsp = sp

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
#获取最新交易日，如果当天是交易日，在16:00后用当天
###############################################################################
def lastopenday():
    df = pd.read_csv(r'd:\selestock\calAll.csv')
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
def dayfileupdate():
    
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


    

        
###############################################################################
#将通达信.day读入pands
###############################################################################
def tdxday2df(gpdm,recent_days=0):
    
    gpdm=sgpdm(gpdm)

    sc = 'sh' if gpdm[0]=='6' else 'sz'
    dayfn =getdrive()+'\\tdx\\'+sc+'lday\\'+sc+gpdm+'.day'

    if os.path.exists(dayfn) :
        return day2df(dayfn,recent_days)
    else :
        #空DF
        cols = ['trade_date','open', 'high', 'low','close','amount','vol','pct_chg','pre_close']
        return pd.DataFrame(columns=cols)


#if __name__ == '__main__':
##    sys.exit()
def daysave(begin_num=0,recent_days=0):
    print('%s Running' % sys.argv[0])
    now1 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now1)
    
    #tushare 通过Python SDK 调取数据
    #https://tushare.pro/document/1?doc_id=131
    
    mytoken='18fcea168f6c1f8621c13bef376e726cf5e31fde3f579db37929181b'
    pro = ts.pro_api(token=mytoken)


    cal=pro.trade_cal(start_date='19901219',end_date='20180505',fields='cal_date,is_open,pretrade_date')
    cal=cal.append(pro.trade_cal(start_date='20180506',end_date='20191231',fields='cal_date,is_open,pretrade_date'))

    cal=cal.sort_values(by='cal_date',ascending=False)
    cal=cal[(cal['is_open']==1)]

    cal=cal[(cal['cal_date']<=today)]
 
    jbxx = pro.stock_basic(fields='ts_code,name,list_date')

    dbfn=getdrive()+'\\tdx\\TDXDAY.db'
    cn = sqlite3.connect(dbfn)
    
    try:    
        
        for i in range(begin_num,len(jbxx)):
            row=jbxx.loc[i]
    #    for index, row in jbxx.iterrows():
            
            gpdm=row[0]
            gpmc=row[1]
            ssrq=row[2]
            print('正在处理第%d个：%s%s,请等待……' % (i+1,gpdm,gpmc))
            
            daydf=tdxday2df(gpdm,recent_days)
            
            #通过复权因子确定交易日期，
#            adjdf = pro.adj_factor(ts_code=gpdm)
            adjdf=cal[(cal['cal_date']>=ssrq)]
            adjdf=adjdf[['cal_date']]
            adjdf.columns=['trade_date'] 
            adjdf=adjdf.assign(ts_code=gpdm)
            daydf=pd.merge(daydf,adjdf,how='right',on='trade_date')
            
            
            daydf=daydf[['ts_code','trade_date','close','pre_close']]
        
            daydf=daydf.sort_values(by='trade_date')
            
            daydf=daydf.assign(is_open=1)
            
            daydf.loc[daydf['close'].isna(),'is_open']=0
            
            daydf['close'].fillna(method='ffill',inplace=True)
        
            daydf.loc[daydf['is_open']==0,'pre_close']=daydf['close']
            
        #    dayadj.to_excel(r'd:\selestock\tmp.xlsx')
        
            daydf=daydf[['ts_code','trade_date','close','pre_close','is_open']]
    
            daydf=daydf.dropna()
            
            data=daydf.values.tolist()
            if len(data)>0:
                cn.executemany('INSERT OR REPLACE INTO DAYCLOSE (TS_CODE,TRADE_DATE,CLOSE,PRE_CLOSE,IS_OPEN) VALUES (?,?,?,?,?)', data)
            if (i+1) % 100 == 0:
                cn.commit()
                
        cn.commit()
        cn.close()    
    except:
        cn.close()
        
    
    now3 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now1)
    print('结束运行时间：%s' % now3)

#if __name__ == '__main__':
#    sys.exit()
def save_csv(recent_days=0):
    cols=['ts_code','trade_date','close','pre_close','is_open']
    dbfn=getdrive()+'\\tdx\\TDXDAY.db'
    cn = sqlite3.connect(dbfn)
    
    try:
        curs = cn.cursor()
        
        sql='''select distinct trade_date from dayclose order by trade_date;'''
        curs.execute(sql)        
        tradedate = curs.fetchall()
        tradedate=[date[0] for date in tradedate]
        
        tradedate.sort(reverse = False)
    
        j = len(tradedate) -recent_days
    #    df=pd.DataFrame(tradedate,columns=['trade_date'])
    #    df=df.sort_values(by='trade_date',ascending=False)
        
        for i in range(j,len(tradedate)):
            rq=tradedate[i]
            sql='''select ts_code, trade_date, close, pre_close, is_open from dayclose where trade_date=%s order by ts_code;''' % rq
            curs.execute(sql)        
            data = curs.fetchall()
     
            csvfn=getdrive()+'\\tdx\\day\\%s.csv' % rq
            df = pd.DataFrame(data,columns=cols)
            df.to_csv(csvfn,index=False)

        cn.close()

    except:
        cn.close()
        
    return

#if __name__ == '__main__':
##    sys.exit()
    now1 = datetime.datetime.now().strftime('%H:%M:%S')

    cols=['ts_code','trade_date','close','pre_close','is_open']
    dbfn=getdrive()+'\\tdx\\TDXDAY.db'
    cn = sqlite3.connect(dbfn)
    curs = cn.cursor()
    rq='20150521'
    sql='''select ts_code, trade_date, close, pre_close, is_open from dayclose where trade_date=%s order by ts_code;''' % rq
    curs.execute(sql)        
    data = curs.fetchall()

    df = pd.DataFrame(data,columns=cols)
    now2 = datetime.datetime.now().strftime('%H:%M:%S')


    cvsfn=getdrive()+'\\tdx\\day\\%s.csv' % rq
    df = pd.read_csv(cvsfn)

    now3 = datetime.datetime.now().strftime('%H:%M:%S')

    print('开始运行时间：%s' % now1)
    print('开始运行时间：%s' % now2)
    print('开始运行时间：%s' % now3)

def day_fns():
    files = os.listdir(getdrive()+'\\tdx\\day')
    fs = [re.findall('(\d{8})\.csv',e) for e in files]

    jyrqlist =[]
    for e in fs:
        if len(e)>0:
            jyrqlist.append(e[0])

    return jyrqlist



if __name__ == '__main__':
#    sys.exit()
        
    now1 = datetime.datetime.now().strftime('%H:%M:%S')
#    daysave(0,3)
#    save_csv(3)

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

    cal=cal[(cal['is_open']==1)]

    cal=cal[(cal['cal_date']<=today)]
    cal=cal.sort_values(by='cal_date',ascending=True)
    
    #提取每日股价csv文件名，查看哪些日期没有数据文件，
    csvfns=day_fns()
    dlcsv=cal[~cal['cal_date'].isin(csvfns)]
 
    
    for index,row in dlcsv.iterrows():
        rq=row[0]
        pre_rq=row[2]

        print(rq,pre_rq)
        
        df = pro.daily(trade_date=rq)
        df = df[['ts_code','trade_date','close','pre_close']]
        df = df.assign(is_open=1)
        
        adjdf = pro.adj_factor(trade_date=rq)  
        
        no_open = adjdf[~(adjdf['ts_code'].isin(df['ts_code']))]
        

        fn=getdrive()+'\\tdx\\day\\%s.csv' % rq
        pre_fn=getdrive()+'\\tdx\\day\\%s.csv' % pre_rq

        no_opendf=pd.read_csv(pre_fn, dtype={'trade_date':'object'})        
        no_opendf=no_opendf[no_opendf['ts_code'].isin(no_open['ts_code'])]
        no_opendf['trade_date']=rq
        no_opendf['pre_close']=no_opendf['close']
        no_opendf['is_open']=0
        
        df=df.append(no_opendf)
        df=df[df['ts_code'].isin(adjdf['ts_code'])]

        df=df.sort_values(by='ts_code',ascending=True)
        
        df.to_csv(fn,index=False)        

        
    now2 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now1)
    print('开始运行时间：%s' % now2)
        