# -*- coding: utf-8 -*-
"""
从大智慧提取股东进出数据导入Sqlite数据库

bt和ffn轻量级量化模块库
http://www.topquant.vip/?p=400

主流的比较流行的Python量化开源框架
https://www.jianshu.com/p/1658f319bfdc


selenium之使用chrome浏览器测试（附chromedriver与chrome的对应关系表）
https://www.cnblogs.com/JHblogs/p/7699951.html

chromedriver
http://chromedriver.storage.googleapis.com/index.html
http://chromedriver.storage.googleapis.com/index.html?path=2.9/
使用python3的Selenium启动chromedriver
https://blog.csdn.net/JavaLixy/article/details/77874715


selenium之 chromedriver与chrome版本映射表（更新至v2.40）
http://blog.csdn.net/huilan_same/article/details/51896672

python+selenium跑chorme时chromedriver放在哪里
https://blog.csdn.net/yinshuilan/article/details/78742728

Python+Selenium与Chrome如何进行完美结合
https://www.cnblogs.com/eternal1025/p/8880245.html

成功配对：
Session info: chrome=67.0.3396.99
Driver info: chromedriver=2.40.565498

http://chromedriver.storage.googleapis.com/index.html?path=2.40/


python + selenium + chrome headless 的一些备忘
https://segmentfault.com/a/1190000013067705

"""
from configobj import ConfigObj
from pyquery import PyQuery as pq
import sys
import os
import re
import time
import datetime
import pandas as pd
import numpy as np
import sqlite3
from selenium import webdriver
from bs4 import BeautifulSoup as bs
import xlwings as xw

########################################################################
#初始化本程序配置文件
########################################################################
def iniconfig():
    inifile = os.path.splitext(sys.argv[0])[0]+'.ini'  #设置缺省配置文件
    return ConfigObj(inifile,encoding='GBK')


#########################################################################
#读取键值
#########################################################################
def readkey(config,key):
    keys = config.keys()
    if keys.count(key) :
        return config[key]
    else :
        return ""

########################################################################
#获取驱动器
########################################################################
def getdrive():
    return sys.argv[0][:2]



########################################################################
#检测是不是可以转换成整数
########################################################################
def str2int(num):
    try:
        return int(num)
    except ValueError:
        return num


########################################################################
#检测是不是可以转换成浮点数
########################################################################
def str2float(num):
    try:
        return float(num)
    except ValueError:
        return num

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
#市场代码
###############################################################################
def scdm(gpdm):
    dm=re.findall('(\d{6})',gpdm)
    
    if len(dm)==0 :
        return None

    dm = dm[0]
    
    return 'SH' if dm[0]=='6' else 'SZ'


###############################################################################
#市场代码
###############################################################################
def minus2none(s):
    return s if s!='-' else None


'''
python使用pyquery库总结 
https://blog.csdn.net/baidu_21833433/article/details/70313839

'''


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

    '''
    WebDriver每次启动一个Firefox的实例时，会生成一个匿名的profile，并不会使用当前Firefox的profile。
    这点一定要注意。比如如果访问被测试的web服务需要通过代理，你想直接设置Firefox的代理是行不通的，因
    为WebDriver启动的Firefox实例并不会使用你这个profile，正确的做法是通过FirefoxProfile来设置。
    '''

    config = iniconfig()
    
    ddir=os.path.join(getdrive(),readkey(config,prefix + 'dldir'))
    dafn = dlfn(ddir)

    kw = readkey(config, prefix + 'kw')
    sele = readkey(config, prefix + 'sl')
    newfn = readkey(config, prefix + 'fn')

    username = readkey(config,'iwencaiusername')
    pwd = readkey(config,'iwencaipwd')

    '''
    chrome_options = webdriver.ChromeOptions() 
    prefs = {'profile.default_content_settings.popups': 0, 'download.default_directory': ddir}
    chrome_options.add_experimental_option('prefs', prefs)
    browser = webdriver.Chrome(chrome_options=chrome_options) 
    '''

    fireFoxOptions = webdriver.FirefoxOptions()
    fireFoxOptions.set_headless()
    profile = webdriver.FirefoxProfile()
    profile.set_preference('browser.download.dir', ddir)
    profile.set_preference('browser.download.folderList', 2)
    profile.set_preference('browser.download.manager.showWhenStarting', False)
    
    #http://www.w3school.com.cn/media/media_mimeref.asp
    profile.set_preference('browser.helperApps.neverAsk.saveToDisk', 'application/vnd.ms-excel')
    
    browser = webdriver.Firefox(firefox_profile=profile,firefox_options=fireFoxOptions)

    
#    browser = webdriver.Firefox()  #调试用
    
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
    time.sleep(5)
    
    newfn = os.path.join(ddir,newfn)
    
    if os.path.exists(newfn):
        os.remove(newfn)

    browser.get("http://www.iwencai.com/stockpick?tid=stockpick&ts=1&qs=result_channell")
    time.sleep(5)
    browser.find_element_by_id("auto").clear()
    browser.find_element_by_id("auto").send_keys(kw)
    browser.find_element_by_id("qs-enter").click()
    time.sleep(30)
    
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
    '''
    https://blog.csdn.net/hbhcxs/article/details/73079494
    '''
    
    js="var q=document.documentElement.scrollTop=0"  
    browser.execute_script(js)  
    time.sleep(3) 
    
    #关闭查询项目选单
    trigger = browser.find_element_by_class_name("showListTrigger")
    trigger.click()
    time.sleep(3)
    
    #导出数据
    elem = browser.find_element_by_class_name("export") 
    #在html中类名包含空格，表示属于多个类
    elem.click() 
    time.sleep(3)

    if os.path.exists(dafn):
        os.rename(dafn,newfn)
            
    browser.quit()



##########################################################################
#读取基金基本信息
##########################################################################
def read_fundinfo(xlsfn,cols):    
    wb = xw.Book(xlsfn)

    c = len(xw.Range('A1').expand('right').columns)

    #修改字段名,去掉日期
    #python3 re模块
    #https://www.cnblogs.com/wenwei-blog/p/7216102.html
    
    for i in range(1,c+1):
        fldn = xw.Range((1,i)).value
        xw.Range((1,i)).value = re.sub('\d{4}\.\d{2}\.\d{2}','',fldn)
            
    #读取数据
    data = wb.sheets[0].range('A1').options(pd.DataFrame, expand='table').value

    '''下面的语句很重要，MultiIndex转换成Index'''
    data.columns=[e[0] for e in data.columns]
    
    ''' 注意：数据列的元素数据类型有两种：str、float，运行下条语句后都变成了numpy.float64'''
    '''下面的语句很重要，运行后面的保留小数位数就不会出错'''
    data=data.replace('--',np.nan)   


    '''保留2位小数必须在data=data.replace(np.nan,'--') 前执行
    注意：执行round(2)必须保证同一列各元素的数据类型是一致的,float和numpy.float64是两种不同的类型
    '''
    data=data.round(2)
    xw.apps[0].quit()


    return data[cols]

##########################################################################
#读取基金基本信息
##########################################################################
def read_xls(xlsfn):    
    wb = xw.Book(xlsfn)

    #读取数据
    data = wb.sheets[0].range('A1').options(pd.DataFrame, expand='table').value

    '''下面的语句很重要，MultiIndex转换成Index'''
    data.columns=[e[0] for e in data.columns]

    data=data.drop(['基金简称'],axis=1)
    xw.apps[0].quit()

    return data
    

def str2float_none(x):
    try:
        return float(x)
    except:
        return None

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

def get_fzl():

    '''
    读取i问财下载的xls文件：最新负债率

    '''
    
    xlsfn = 'd:\\selestock\\fzl.xls'
    coldic={'股票代码':'gpdm',	
        '股票简称':'gpmc',	
        '资产负债率(%)'	:'fzl_'
        }
    
    colfloat=['fzl_']
    keycol='gpdm'
    
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

def dgdzjcmx():
    '''
    CREATE TABLE [DGDZJCMX](
      [GPDM] TEXT NOT NULL, 
      [GPMC] TEXT NOT NULL, 
      [BDRQ] TEXT NOT NULL, 
      [BDGS] REAL NOT NULL, 
      [BDGSZB] REAL, 
      [BDJJ] REAL, 
      [BDSZ] REAL, 
      [ZJCFX] TEXT, 
      [GDMC] TEXT, 
      [GDLX] TEXT, 
      [SFGWZJC] TEXT, 
      [GDXZ] TEXT, 
      [ZJCFS] TEXT, 
      [GGRQ] TEXT);
    
    CREATE UNIQUE INDEX [GPDM_BDRQ_GDMC_DGDZJCMX]
    ON [DGDZJCMX](
      [GPDM], 
      [BDRQ], 
      [GDMC]);

        
    gpdm股票代码	
    gpmc股票简称	
    bdrq变动日期 2017.07.13-2018.07.12	
    bdgs大股东变动股数(股)2017.07.13-2018.07.12	
    bdgszb变动数量占流通股比(%)2017.07.13-2018.07.12	

    bgjj变动均价(元)2017.07.13-2018.07.12	
    bdsz变动市值(元)2017.07.13-2018.07.12	
    zjcfx大股东增减持方向2017.07.13-2018.07.12	
    gdmc增减持股东名称2017.07.13-2018.07.12	
    gdlx增减持股东类型2017.07.13-2018.07.12	

    sfgwzjc是否在高位增减持2017.07.13-2018.07.12	
    gdxz股东性质2017.07.13-2018.07.12	
    zjcfs股东增减持方式2017.07.13-2018.07.12	
    ggrq大股东增减持公告日期2017.07.13-2018.07.12

     '''   

    xlsfn = 'd:\\selestock\\gdzjcmx.xls'
    coldic={'股票代码':'gpdm',	
        '股票简称':'gpmc',	
        '变动日期':'bdrq',
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
    
    colfloat={'bdgs','bdgszb','bgjj','bdsz'}
    colsele=[coldic[e] for e in coldic]
    
    df=xls2pd(xlsfn,coldic,colfloat,colsele)


    df.loc[df['bdjj']==0,'bdjj']=None
    df['rq']=df['bdrq'].map(lambda x:x[:4]+'-'+x[4:6]+'-'+x[6:])

    cols=['gpdm','gpmc','rq','bdgs','bdgszb',
          'bdjj','bdsz','zjcfx','gdmc','gdlx',
          'sfgwzjc','gdxz','zjcfs','ggrq']
    
    df=df[cols]
    
    data=np.array(df).tolist()    

    dbfn=getdrive()+'\\hyb\\STOCKDSTX.db'
    dbcn = sqlite3.connect(dbfn)
    
    dbcn.executemany('''INSERT OR REPLACE INTO DGDZJCMX 
                     (GPDM,GPMC,BDRQ,BDGS,BDGSZB,
                     BDJJ,BDSZ,ZJCFX,GDMC,GDLX,
                     SFGWZJC,GDXZ,ZJCFS,GGRQ) 
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', data)

    dbcn.commit()
    
    '''
    Python pandas DataFrame 某一列中添加字符串
    https://blog.csdn.net/sscc_learning/article/details/76993816
    
    pandas的DataFrame怎么把几列数据合并成为新的一列
    https://blog.csdn.net/gangyin5071/article/details/79601386
    
    '''
    
    df['ts1']=['大股东%s' % s for s in df['zjcfx']]
    df['ts2']=df['gdlx']+df['gdmc']+df['zjcfs']+df['zjcfx']+df['bdgs'].map(lambda x:str(x/10000)+'万股')
    df['tslx']='0'
    
    cols=['gpdm','rq','ts1','ts2','tslx']
    df=df[cols]
    data=np.array(df).tolist()    

    dbcn.executemany('INSERT OR REPLACE INTO THS (GPDM,RQ,TS1,TS2,TSLX) VALUES (?,?,?,?,?)', data)
    dbcn.commit()
    
    dbcn.close()

    return


def xsjj():
    '''
    CREATE TABLE [XSJJ](
      [GPDM] TEXT NOT NULL, 
      [GPMC] TEXT NOT NULL, 
      [JJRQ] TEXT NOT NULL, 
      [JJGS] REAL NOT NULL, 
      [JJZB] REAL, 
      [JJSZ] REAL, 
      [CKRQ] TEXT, 
      [JJGLX] TEXT);
    
    CREATE UNIQUE INDEX [GPDM_JJRQ_JJGLX_XSJJ]
    ON [XSJJ](
      [GPDM], 
      [JJRQ], 
      [JJGLX]);


    股票代码	股票简称	解禁日期	解禁股数(股)	解禁比例(%)	解禁金额(元)	解禁计算参考时间	解禁股类型

    '''
    xlsfn = 'd:\\selestock\\xsjj.xls'
    coldic={'股票代码':'gpdm',	
        '股票简称':'gpmc',	
        '解禁日期':'jjrq',
        '解禁股数(股)':'jjgs',	
        '解禁比例(%)':'jjzb',	
    
        '解禁金额(元)':'jjsz',	
        '解禁计算参考时间':'ckrq',	
        '解禁股类型':'jjglx'}
    
    colfloat={'jjgs','jjzb','jjsz'}

    colsele=[coldic[e] for e in coldic]
    
    df=xls2pd(xlsfn,coldic,colfloat,colsele)

    df['rq']=df['jjrq'].map(lambda x:x[:4]+'-'+x[4:6]+'-'+x[6:])

    cols=['gpdm','gpmc','rq','jjgs','jjzb',
          'jjsz','ckrq','jjglx']
    
    df=df[cols]
    
    data=np.array(df).tolist()    

    dbfn=getdrive()+'\\hyb\\STOCKDSTX.db'
    dbcn = sqlite3.connect(dbfn)
    
    dbcn.executemany('''INSERT OR REPLACE INTO XSJJ 
                     (GPDM,GPMC,JJRQ,JJGS,JJZB,
                     JJSZ,CKRQ,JJGLX) 
                    VALUES (?,?,?,?,?,?,?,?)''', data)

    dbcn.commit()

    df['ts1']='限售解禁'
    df['ts2']=df['jjglx']+df['jjgs'].map(lambda x:str(x/10000)+'万股解禁，占总股本')+df['jjzb'].map(lambda x:str(x)+'%')
    df['tslx']='0'
    
    today=datetime.datetime.now().strftime("%Y-%m-%d")
    df.loc[df['rq']>today,'tslx']='1'    
    
    cols=['gpdm','rq','ts1','ts2','tslx']
    df=df[cols]
    data=np.array(df).tolist()    

    dbcn.executemany('INSERT OR REPLACE INTO THS (GPDM,RQ,TS1,TS2,TSLX) VALUES (?,?,?,?,?)', data)
    dbcn.commit()
    
    dbcn.close()

    return

def ggzjc():
    '''
    CREATE TABLE [GGZJC](
      [GPDM] TEXT NOT NULL, 
      [GPMC] TEXT NOT NULL, 
      [BDRQ] TEXT NOT NULL, 
      [GGMC] TEXT NOT NULL, 
      [GGRQ] TEXT,
      
      [GGZW] TEXT,
      [BDLX] TEXT,
      [BDSZ] REAL, 
      [BDGS] REAL, 
      [BDHCG] REAL, 

      [BDJJ] REAL, 
      [QLTZB] REAL, 
      [XGGG] TEXT, 
      [YGGGX] TEXT);
    
    CREATE UNIQUE INDEX [GPDM_BDRQ_GGMC_GGZJC]
    ON [GGZJC](
      [GPDM], 
      [BDRQ], 
      [GGMC]);


    股票代码	
    股票简称	
    变动日期 2017.07.12-2018.07.11	
    高管名称 2017.07.12-2018.07.11	
    高管增减持公告日期2017.07.12-2018.07.11	
    
    增减持高管职务2017.07.12-2018.07.11	
    变动类型 2017.07.12-2018.07.11	
    高管变动市值 (元)2017.07.12-2018.07.11
    高管变动股数 (股)2017.07.12-2018.07.11	
    变动后持股数 (股)2017.07.12-2018.07.11
    
    高管变动均价 (元)2017.07.12-2018.07.11	
    变动占比 (%)2017.07.12-2018.07.11	前流通占比
    相关董监高 2017.07.12-2018.07.11	
    变动人与董监高关系2017.07.12-2018.07.11

    '''
    xlsfn = 'd:\\selestock\\ggzjc.xls'
    coldic={'股票代码':'gpdm',	
        '股票简称':'gpmc',	
        '变动日期':'bdrq',
        '高管名称':'ggmc',	
        '高管增减持公告日期':'ggrq',	
    
        '增减持高管职务':'ggzw',
        '变动类型':'bdlx',	
        '高管变动市值(元)':'bdsz',
        '高管变动股数(股)':'bdgs',
        '变动后持股数(股)':'bdhcg',
        
        '高管变动均价(元)':'bdjj',	
        '变动占比(%)':'qltzb',
        '相关董监高':'xggg',
        '变动人与董监高关系':'ygggx'
        }
    
    colfloat={'bdsz','bdgs','bdhcg','bdjj','qltzb'}

    colsele=[coldic[e] for e in coldic]
    
    df=xls2pd(xlsfn,coldic,colfloat,colsele)

    df['rq']=df['bdrq'].map(lambda x:x[:4]+'-'+x[4:6]+'-'+x[6:])

    cols=['gpdm','gpmc','rq','ggmc','ggrq',
          'ggzw','bdlx','bdsz','bdgs','bdhcg',
          'bdjj','qltzb','xggg','ygggx']
    
    df=df[cols]
    
    data=np.array(df).tolist()    

    dbfn=getdrive()+'\\hyb\\STOCKDSTX.db'
    dbcn = sqlite3.connect(dbfn)
    
    dbcn.executemany('''INSERT OR REPLACE INTO GGZJC 
                     (GPDM,GPMC,BDRQ,GGMC,GGRQ,
                     GGZW,BDLX,BDSZ,BDGS,BDHCG,
                     BDJJ,QLTZB,XGGG,YGGGX) 
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', data)

    dbcn.commit()

    df['ts1']=['高管%s' % ('增持' if zjc>0 else '减持') for zjc in df['bdgs']]
    
    df['ts2']=df['ggzw']+df['ggmc']+df['bdlx']+df['bdgs'].map(lambda x:('增持' if x>0 else '减持')+str(x/10000)+'万股，占前流通')+df['qltzb'].map(lambda x:str(x)+'%')
    df['tslx']='0'
    
    today=datetime.datetime.now().strftime("%Y-%m-%d")
    df.loc[df['rq']>today,'tslx']='1'    
    
    cols=['gpdm','rq','ts1','ts2','tslx']
    df=df[cols]
    data=np.array(df).tolist()    

    dbcn.executemany('INSERT OR REPLACE INTO THS (GPDM,RQ,TS1,TS2,TSLX) VALUES (?,?,?,?,?)', data)
    dbcn.commit()
    
    dbcn.close()

    return


def hgmx():
    '''
    CREATE TABLE [HGMX](
      [GPDM] TEXT NOT NULL, 
      [GPMC] TEXT NOT NULL, 
      [JZRQ] TEXT NOT NULL, 
      [GGRQ] TEXT NOT NULL, 
      [LJHGGS] REAL NOT NULL, 
      [LJHGZGBZB] REAL, 
      [HGZGJ] REAL, 
      [HGZDJ] REAL, 
      [HGZJE] REAL, 
      [OK] TEXT);
    
    CREATE UNIQUE INDEX [GPDM_GGRQ_HGMX]
    ON [HGMX](
      [GPDM], 
      [GGRQ]);

    '''
    xlsfn = 'd:\\selestock\\hgmx.xls'
    coldic={'股票代码':'gpdm',	
        '股票简称':'gpmc',	
        '回购截止日期':'jzrq',
        '回购公告日期':'ggrq',	
        '累计回购数量(股)':'ljhggs',	
    
        '累计回购比例(%)':'ljhgzgbzb',	
        '股份回购每股最高价格(元)':'hgzgj',	
        '股份回购每股最低价格(元)':'hgzdj',
        '股份回购支付金额(元)':'hgzje',
        '回购是否完成':'ok'}
    
    colfloat={'ljhggs','ljhgzgbzb','hgzgj','hgzdj','hgzje'}

    colsele=[coldic[e] for e in coldic]
    
    df=xls2pd(xlsfn,coldic,colfloat,colsele)
    
    #去掉还没有实施的
    df.loc[df['ljhggs']==0,'ljhggs']=None
    df=df.dropna(subset=['jzrq', 'ggrq','ljhggs'])
    
    df['rq']=df['ggrq'].map(lambda x:x[:4]+'-'+x[4:6]+'-'+x[6:])

    cols=['gpdm','gpmc','jzrq','rq','ljhggs',
          'ljhgzgbzb','hgzgj','hgzdj','hgzje','ok']
    
    df=df[cols]

    df.loc[df['hgzgj']==0,'hgzgj']=None
    df.loc[df['hgzdj']==0,'hgzdj']=None

    
    data=np.array(df).tolist()    

    dbfn=getdrive()+'\\hyb\\STOCKDSTX.db'
    dbcn = sqlite3.connect(dbfn)
    
    dbcn.executemany('''INSERT OR REPLACE INTO HGMX 
                     (GPDM,GPMC,JZRQ,GGRQ,LJHGGS,
                      LJHGZGBZB,HGZGJ,HGZDJ,HGZJE,OK) 
                    VALUES (?,?,?,?,?,?,?,?,?,?)''', data)

    dbcn.commit()

    df['ts1']='回购实施公告'
    df['ts2']=df['ljhggs'].map(lambda x:'累计回购'+str(x/10000)+'万股，占总股本')+df['ljhgzgbzb'].map(lambda x:str(x)+'%，支付')+df['hgzje'].map(lambda x:str(round(x/100000000,4))+'亿元。回购是否完成:')+df['ok']
    df['tslx']='0'
    
    today=datetime.datetime.now().strftime("%Y-%m-%d")
    df.loc[df['rq']>today,'tslx']='1'    
    
    cols=['gpdm','rq','ts1','ts2','tslx']
    df=df[cols]
    data=np.array(df).tolist()    

    dbcn.executemany('INSERT OR REPLACE INTO THS (GPDM,RQ,TS1,TS2,TSLX) VALUES (?,?,?,?,?)', data)
    dbcn.commit()
    
    dbcn.close()

    return

def hggg():
    '''
    CREATE TABLE [HGGG](
      [GPDM] TEXT NOT NULL, 
      [GPMC] TEXT NOT NULL, 
      [HGYAGGRQ] TEXT NOT NULL, 
      [ZXGGRQ] TEXT NOT NULL, 
      [FAJD] TEXT NOT NULL, 
      [NHGZJE] REAL, 
      [HGQX] REAL, 
      [HGKSRQ] TEXT, 
      [HGJZRQ] TEXT);
    
    CREATE UNIQUE INDEX [GPDM_HGYAGGRQ_ZXGGRQ_HGGG]
    ON [HGGG](
      [GPDM], 
      [HGYAGGRQ],
      [ZXGGRQ]);

    股票代码	
    股票简称	
    回购董事会预案公告日	
    回购最新公告日	
    回购方案进度	
    
    拟回购资金总额(元)	
    回购股份期限(月)	
    回购开始日	
    回购截止日
    
    
    董事会预案
    股东大会通过
    回购股份已注销
    回购完成
    实施回购
    停止回购


    '''
    xlsfn = 'd:\\selestock\\hggg.xls'
    coldic={'股票代码':'gpdm',	
        '股票简称':'gpmc',	
        '回购董事会预案公告日':'hgyaggrq',
        '回购最新公告日':'zxggrq',	
        '回购方案进度':'fajd',	
    
        '拟回购资金总额(元)':'nhgzje',	
        '回购股份期限(月)':'hgqx',	
        '回购开始日':'hgksrq',
        '回购截止日':'hgjzrq'}
    
    colfloat=['nhgzje','hgqx']

    colsele=[coldic[e] for e in coldic]
    
    df=xls2pd(xlsfn,coldic,colfloat,colsele)
    
    
    df['hgyaggrq']=df['hgyaggrq'].map(lambda x:x[:4]+'-'+x[4:6]+'-'+x[6:])
    df['zxggrq']=df['zxggrq'].map(lambda x:x[:4]+'-'+x[4:6]+'-'+x[6:])

    cols=['gpdm','gpmc','hgyaggrq','zxggrq','fajd',
          'nhgzje','hgqx','hgksrq','hgjzrq']
    
    df=df[cols]
    
    data=np.array(df).tolist()    

    dbfn=getdrive()+'\\hyb\\STOCKDSTX.db'
    dbcn = sqlite3.connect(dbfn)
    
    dbcn.executemany('''INSERT OR REPLACE INTO HGGG 
                     (GPDM,GPMC,HGYAGGRQ,ZXGGRQ,FAJD,
                      NHGZJE,HGQX,HGKSRQ,HGJZRQ) 
                    VALUES (?,?,?,?,?,?,?,?,?)''', data)

    dbcn.commit()
    
    #预案公告
    df1=df.loc[:,['gpdm','hgyaggrq','nhgzje']]
    
    df1['ts1']='回购预案公告'
    df1['ts2']=df['nhgzje'].map(lambda x:'拟回购总金额'+str(round(x/100000000,4))+'亿元。')
    df1['tslx']='0'

    cols=['gpdm','hgyaggrq','ts1','ts2','tslx']
    df1=df1[cols]
    data=np.array(df1).tolist()    

    dbcn.executemany('INSERT OR REPLACE INTO THS (GPDM,RQ,TS1,TS2,TSLX) VALUES (?,?,?,?,?)', data)
    dbcn.commit()
    
    #最新公告
    df1=df.loc[(df['fajd']!='董事会预案'),['gpdm','zxggrq','fajd','nhgzje','hgksrq','hgjzrq']]
    
    df1['ts1']=df['fajd']
    df1['ts2']=df['fajd']+df['hgksrq'].map(lambda x:',回购起止日期:'+(x if x!=None else '')+'-')+df['hgjzrq']
    df1['tslx']='0'
    
    df1.loc[(df['fajd']=='股东大会通过'),'ts1']='回购方案通过'    
    df1.loc[(df['fajd']=='股东大会通过'),'ts2']=df['nhgzje'].map(lambda x:'拟回购总金额'+str(round(x/100000000,4))+'亿元。')
    
    cols=['gpdm','zxggrq','ts1','ts2','tslx']
    df1=df1[cols]
    data=np.array(df1).tolist()    

    dbcn.executemany('INSERT OR REPLACE INTO THS (GPDM,RQ,TS1,TS2,TSLX) VALUES (?,?,?,?,?)', data)
    dbcn.commit()
    
    dbcn.close()

    return




if __name__ == "__main__":  
#def main():
    
    now1 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now1)

#    dl_ths_xls('xsjj')
#    dl_ths_xls('ggzjc')
#    dl_ths_xls('dgdzjcmx')
#    dl_ths_xls('dgdzjcgg')

#    dl_ths_xls('hggg')
#    dl_ths_xls('hgmx')
#    dl_ths_xls('zyhz')     #质押汇总
#    dl_ths_xls('fzl')     #负债率
    
#
#    dgdzjcmx()    
#    xsjj()
#    ggzjc()
#    hgmx()
#    hggg()
    
    
    now2 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now1)
    print('结束运行时间：%s' % now2)

