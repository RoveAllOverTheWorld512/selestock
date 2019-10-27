# -*- coding: utf-8 -*-
"""
功能：本程序从中财网提取研报数据，保存sqlite
用法：每周运行
"""

import struct
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sqlite3
import os
import sys
import re
import pandas as pd
import winreg

'''
CREATE TABLE [CFI_YB](
  [GPDM] TEXT NOT NULL, 
  [RQ] TEXT NOT NULL, 
  [JG] TEXT NOT NULL, 
  [NF] TEXT NOT NULL, 
  [EPS] REAL NOT NULL);

CREATE UNIQUE INDEX [GPDM_RQ_JG_NF_CFIYB]
ON [CFI_YB](
  [GPDM], 
  [RQ], 
  [JG], 
  [NF]);

CREATE TABLE [CFI_YBURL](
  [GPDM] TEXT PRIMARY KEY NOT NULL, 
  [YBURL] TEXT NOT NULL, 
  [LASTDATE] TEXT NOT NULL);

'''

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

###############################################################################
#万亿转换
###############################################################################
def wyzh(str):
    wy=re.findall('(.+)亿',str)
    if len(wy)==1 :
        return float(wy[0])*100000000
    wy=re.findall('(.+)万',str)
    if len(wy)==1 :
        return float(wy[0])*10000

    return 0

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
    gpdmb=gpdmb.set_index('gpdm')
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


def getdrive():
    return sys.argv[0][:2]

###############################################################################
#最近15天内读取过研报的股票代码表
###############################################################################
def del_gpdm(n):

    td=datetime.datetime.now()
    m1=(td+datetime.timedelta(n)).strftime("%Y-%m-%d")

    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    sql='select distinct gpdm from cfi_yburl where lastdate>"%s";' % m1
    curs.execute(sql)        
    data = curs.fetchall()

    cols = ['gpdm']
    
    df=pd.DataFrame(data,columns=cols)
    df=df.set_index('gpdm')
    
    return df

###############################################################################
#中财网研报地址字典
###############################################################################
def yburldic():
    urldic={}
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    sql='select gpdm,yburl from cfi_yburl;'
    curs.execute(sql)        
    data = curs.fetchall()

    for gpdm,yburl in data:
        urldic[gpdm]=yburl

    
    return urldic


###############################################################################
#从中财网读取研报数据
###############################################################################
def get_cfiyb(dbcn,gpdm,urldic,lastrq):
    pjjgdic=jgdic()
    browser = webdriver.PhantomJS()

    dturl=[]
    today = datetime.datetime.now().strftime('%Y-%m-%d')

    #没有研报地址的，查找研报地址
    if not gpdm in urldic.keys():
        
        
        url='http://quote.cfi.cn/%s.html' % sgpdm(gpdm)
        try :            
            browser.get(url)
            
            '''
            Python selenium —— 父子、兄弟、相邻节点定位方式详解
            https://blog.csdn.net/huilan_same/article/details/52541680
            
            selenium webdriver三种等待方法
            https://www.cnblogs.com/lgh344902118/p/6015593.html
            
            python selenium 显示等待WebDriverWait与条件判断expected_conditions举例
            https://blog.csdn.net/wycaoxin3/article/details/74017971
            
            Selenium 高阶应用之WebDriverWait 和 expected_conditions
            https://www.cnblogs.com/yicaifeitian/p/4749149.html
            
            selenium用法详解
            https://www.cnblogs.com/themost/p/6900852.html
            '''
            
            elem=WebDriverWait(browser, 3).until(
                    EC.presence_of_element_located((By.XPATH, "//a[text()='研报一览']")))
            
            url = elem.get_attribute('href')
            dturl.append([lgpdm(gpdm),url,today,0])

            dbcn.executemany('''INSERT OR REPLACE INTO CFI_YBURL (GPDM,YBURL,LASTDATE,OK) 
                            VALUES (?,?,?,?)''', dturl)

        except :
            print('1异常退出，无法找到研报地址。')
            browser.quit()
            return False

    else :
        url=urldic[gpdm]

    try:

        browser.get(url)
        elem=WebDriverWait(browser, 3).until(
                EC.presence_of_element_located((By.ID, "tabh")))

#        elem = browser.find_element_by_id("tabh")
        tblrows = elem.find_elements_by_tag_name('tr')
    except :
        print('2异常退出，研报地址有误。')
        browser.quit()

        return False
        
    #遍历行
    data = []
    for j in range(2,len(tblrows)):
        
        try :    
            tblcols = tblrows[j].find_elements_by_tag_name('td')
            rq = tblcols[0].text

            if lastrq!=None and rq<lastrq :
                break
            
            pjjg = tblcols[1].text
            if pjjg in pjjgdic.keys():
                pjjg=pjjgdic[pjjg]
            
            for i in range(2,5):
                yc = tblcols[i].text
                if yc!='--':
                    nf=yc[:4]
                    eps=float(yc[5:])
                    data.append([lgpdm(gpdm),rq,pjjg,nf,eps])
        except:
            print('3异常退出，读取研报出错。')
            browser.quit()
            return False
        
        
            
    if len(data)>0:
        dbcn.executemany('''INSERT OR REPLACE INTO CFI_YB (GPDM,RQ,JG,NF,EPS) 
                            VALUES (?,?,?,?,?)''', data)

    dturl.append([lgpdm(gpdm),url,today,1])
    dbcn.executemany('''INSERT OR REPLACE INTO CFI_YBURL (GPDM,YBURL,LASTDATE,OK) 
                            VALUES (?,?,?,?)''', dturl)
 
    dbcn.commit()
    browser.quit()
            
    return True

###############################################################################
#获取中财网研报最后提取日期
###############################################################################
def cfiyb_lastdate():
    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    
    #提取年报以来最新股东户数    
    curs.execute('''select gpdm,rq from cfi_yb order by rq desc;''')
    
    data = curs.fetchall()
    
    #保留最新户数
    df=pd.DataFrame(data,columns=['gpdm','rq'])
    df = df.drop_duplicates(['gpdm'],keep='first')
    df=df.set_index('gpdm')
    dic=df.to_dict('index')
    
    return {e:dic[e]['rq'] for e in dic.keys()}

if __name__ == "__main__": 
    print('%s Running' % sys.argv[0])

    now1 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now1)


    dbfn=getdrive()+'\\hyb\\STOCKEPS.db'
    dbcn = sqlite3.connect(dbfn)
    
    gpdmb=get_gpdm()
    urldic=yburldic()
    lastdate=cfiyb_lastdate()

    delgpdm=del_gpdm(-2)    
    gplb=gpdmb[~gpdmb.index.isin(delgpdm.index)]  
    gplb=gplb.index

#    gplb=zxglst()            

    for i in range(len(gplb)):
        gpdm=gplb[i]
        gpmc = gpdmb.loc[gpdm]['gpmc']

        lastrq=None
        if gpdm in lastdate.keys():
            lastrq=lastdate[gpdm]

        now = datetime.datetime.now().strftime('%H:%M:%S')

        print("%s  共有%d只股票，正在处理第%d只：%s%s，请等待…………" % (now,len(gplb),i+1,gpdm,gpmc)) 
        if get_cfiyb(dbcn,gpdm,urldic,lastrq):
            print('          %s成功' % gpdm)
        else:
            print('          %s失败' % gpdm)
            
    
#    browser.quit()
    dbcn.close()
    
    now2 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now1)
    print('结束运行时间：%s' % now2)
    

    
    
