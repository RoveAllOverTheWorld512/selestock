# -*- coding: utf-8 -*-
"""
从大智慧F10提取十大流通股东数据导入Sqlite数据库
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pyquery import PyQuery as pq
import datetime
import time
import sqlite3
import sys
import os
import re
import pandas as pd
import winreg
import traceback

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
def gettdxdir():

    try :
        key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE,r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\华西证券华彩人生")
        value, type = winreg.QueryValueEx(key, "InstallLocation")
    except :
        print("本机未安装【华西证券华彩人生】软件系统。")
        sys.exit()
    return value

########################################################################
#从大智慧网F10获取股东户数
########################################################################
def get_gdhs(gpdm):

    sc=scdm(gpdm)
    gpdm=sgpdm(gpdm)
    
    data=[]
    url = 'http://webf10.gw.com.cn/'+sc+'/B10/'+sc+gpdm+'_B10.html'

    try :
        html = pq(url,encoding="utf-8")
        #第3个区块
        #sect = pq(html('section').eq(2).html())
        #提取预测明细
        sect=html('section').filter('#股东人数').html()
        tr=pq(sect)
    except : 
        print("出错退出")
        return data

    for i in range(1,len(tr('ul'))):
        
        il=tr('ul').eq(i).text().split(' ')
        rq=il[0]
        gdhs=il[1]

        data.append([lgpdm(gpdm),rq,gdhs])

    return data
    
########################################################################
#从大智慧网F10获取限售解禁
########################################################################
def get_xsjj(gpdm):

    sc=scdm(gpdm)
    gpdm=sgpdm(gpdm)
    
    data=[]
    url = 'http://webf10.gw.com.cn/'+sc+'/B11/'+sc+gpdm+'_B11.html'

    try :
        html = pq(url,encoding="utf-8")
        #第3个区块
        #sect = pq(html('section').eq(2).html())
        #提取预测明细
        sect=html('section').filter('#解禁流通').html()
                 
        tbl=pq(sect)
        tr=pq(tbl('table').eq(1).html())
        
    except : 
        print("出错退出")
        return data

    for i in range(1,len(tr('tr'))):
        
        td=tr('tr').eq(i).text().split(' ')
        jjrq=td[0].replace('/','-')
        bcjj=round(float(td[1])/10000,4)
        wlt=float(td[6])/10000
        try:
            qltbl=float(td[3].replace('%',''))
            qlt=round(bcjj/qltbl*100,4)
            hlt=round(qlt+bcjj,4)
            hltbl=round(bcjj/hlt*100,4)
        except:            
            qltbl=None
            qlt=None
            hlt=None
            hltbl=None
            
        data.append([lgpdm(gpdm),jjrq,bcjj,qlt,qltbl,hlt,hltbl,None,None,wlt])

    return data
 
########################################################################
#从大智慧网F10获取高管股东持股变动
########################################################################
def get_gggdcgbd(gpdm):
    sc=scdm(gpdm)
    gpdm=sgpdm(gpdm)
    
    data=[]
    url = 'http://webf10.gw.com.cn/'+sc+'/B10/'+sc+gpdm+'_B10.html'

    try :
        html = pq(url,encoding="utf-8")

        sect=html('section').filter('#高管持股变动').html()
                 
        tbl=pq(sect)

        if len(tbl('tr'))>1:
            for i in range(1,len(tbl('tr'))):
                
                row=pq(tbl('tr').eq(i))
                bdrq=row('td').eq(0).text().replace('/','-')
                bdr=row('td').eq(1).text().split(' ')[0]
                bdfx=row('td').eq(3).text()[:2]
                try:
                    bdsl=float(row('td').eq(3).text()[2:])
                    data.append([lgpdm(gpdm),bdrq,bdr,bdsl,bdfx])
                except:
                    pass
    except : 
        print("高管持股变动,出错退出")


    try :
        html = pq(url,encoding="utf-8")

        sect=html('section').filter('#股东持股变动').html()
                 
        tbl=pq(sect)
        
        if len(tbl('tr'))>1:
            for i in range(1,len(tbl('tr'))):
                
                row=pq(tbl('tr').eq(i))
                
                bdrq=row('td').eq(0).text().replace('/','-')
                bdr=row('td').eq(1).text()
                bdfx=row('td').eq(3).text()
                try:
                    bdsl=float(row('td').eq(2).text())*10000
                    bdsl=-bdsl if bdfx=='减持' else bdsl
                    data.append([lgpdm(gpdm),bdrq,bdr,bdsl,bdfx])
                except:
                    pass
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

        
CREATE TABLE [GGGDCGBD](
  [GPDM] TEXT NOT NULL, 
  [BDRQ] TEXT NOT NULL, 
  [BDR] TEXT NOT NULL, 
  [BDSL] REAL NOT NULL, 
  [BDFX] TEXT NOT NULL);

CREATE UNIQUE INDEX [GPDM_BDRQ_BDR_GGGDCGBD]
ON [GGGDCGBD](
  [GPDM], 
  [BDRQ], 
  [BDR]);

'''    


########################################################################
#从大智慧网F10获取流通股
########################################################################
def get_ltg1(browser,gpdm):

    sc=scdm(gpdm)
    gpdm=sgpdm(gpdm)

#    fireFoxOptions = webdriver.FirefoxOptions()
#    fireFoxOptions.set_headless()
#    browser = webdriver.Firefox(firefox_options=fireFoxOptions)
#    browser = webdriver.Firefox()

    url = 'http://webf10.gw.com.cn/'+sc+'/B10/'+sc+gpdm+'_B10.html'

    browser.get(url)
    browser.find_element_by_xpath("//a[text()='十大流通股东']").click()

    try:
        rqlst = WebDriverWait(browser, 20).until(
            EC.presence_of_element_located((By.ID, "sdgd_nav")))
    except:
        f=open(r"d:\selestock\log.txt",'a')  
        traceback.print_exc(file=f)  
        f.flush()  
        f.close()
        return []
                
#    rqlst = browser.find_element_by_id("timeList")

    rqs = rqlst.find_elements_by_tag_name("a")
    data=[]
    for i in range(len(rqs)):
        elem=rqs[i]
        rq=elem.text
        elem.click()
        try:
            divid='sdltgdCon_' + rq
            div=browser.find_element_by_id(divid)
            ljcg=div.find_element_by_tag_name("p").text
            cg=re.findall('累计持有：(.+)万股，累计占总股比：(.+)\%',ljcg)
            dgdcg=eval(cg[0][0])
            dgdzb=eval(cg[0][1])
            ltg=round(dgdcg/dgdzb*100,2)
            shcg=round(ltg-dgdcg,2)
            data.append([lgpdm(gpdm),rq,ltg,dgdcg,dgdzb,shcg])
        except:
            pass
        
#    browser.quit()        
    return data


########################################################################
#从大智慧网F10获取流通股信息
########################################################################
def get_ltg(browser,gpdm):

    '''
    CREATE TABLE [LTG](
      [GPDM] TEXT NOT NULL, 
      [RQ] TEXT NOT NULL, 
      [LTG] REAL, 
      [SDGDCG] REAL, 
      [SDGDZB] REAL, 
      [SHCG] REAL);
    
    CREATE UNIQUE INDEX [GPDM_RQ_LTG]
    ON [LTG](
      [GPDM], 
      [RQ]);

    '''

    sc=scdm(gpdm)
    gpdm=sgpdm(gpdm)

#    fireFoxOptions = webdriver.FirefoxOptions()
#    fireFoxOptions.set_headless()
#    browser = webdriver.Firefox(firefox_options=fireFoxOptions)
#    browser = webdriver.Firefox()

    url='http://webf10.gw.com.cn/B10_detail.html?stockCode='+sc+gpdm 

    browser.get(url)
    try:
        rqlst = WebDriverWait(browser, 20).until(
            EC.presence_of_element_located((By.ID, "timeList")))
    except:
        f=open(r"d:\selestock\log.txt",'a')  
        traceback.print_exc(file=f)  
        f.flush()  
        f.close()
        return []
                
#    rqlst = browser.find_element_by_id("timeList")

    rqs = rqlst.find_elements_by_tag_name("li")
    data=[]
    for i in range(len(rqs)):
        elem=rqs[i]
        rq=elem.text
        elem.click()
        try:
            ljcg=browser.find_element_by_id("ljcg").text
            cg=re.findall('累计持有：(.+)万股，累计占总流通股比：(.+)\%',ljcg)
            dgdcg=eval(cg[0][0])
            dgdzb=eval(cg[0][1])
            ltg=round(dgdcg/dgdzb*100,2)
            shcg=round(ltg-dgdcg,2)
            data.append([lgpdm(gpdm),rq,ltg,dgdcg,dgdzb,shcg])
        except:
            pass
        
#    browser.quit()        
    return data

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
#读取过研报的股票代码表
###############################################################################
def del_gpdm(rq):

    dbfn=getdrive()+'\\hyb\\STOCKDATA.db'
    dbcn = sqlite3.connect(dbfn)
    curs = dbcn.cursor()
    sql='select distinct gpdm from ltg where rq>="%s";' % rq
    curs.execute(sql)        
    data = curs.fetchall()

    cols = ['gpdm']
    
    df=pd.DataFrame(data,columns=cols)
    df=df.set_index('gpdm')
    
    return df

###############################################################################
#预约披露日期
###############################################################################
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


if __name__ == "__main__":  
#def temp():
    now1 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now1)

#    sys.exit()
    
    fireFoxOptions = webdriver.FirefoxOptions()
    fireFoxOptions.set_headless()
    browser = webdriver.Firefox(firefox_options=fireFoxOptions)

    gpdmb=get_gpdm()
 
    #去掉已提取的
#    delgpdm=del_gpdm('2018-06-30')    
#    gpdmb=gpdmb[~gpdmb.index.isin(delgpdm.index)]  
#
#    td = datetime.datetime.now().strftime('%Y-%m-%d')
#    td1=(datetime.datetime.now()+datetime.timedelta(-10)).strftime("%Y-%m-%d")
#    yyrq=get_yyrq()
#    yyrq=yyrq[yyrq['yyrq']>=td1]
#    yyrq=yyrq[yyrq['yyrq']<=td]
#
#    gpdmb=gpdmb[gpdmb.index.isin(yyrq.index)]


    #自选股
#    zxg=zxglst()
#    gpdmb=gpdmb[gpdmb.index.isin(zxg)]

    dbfn=getdrive()+'\\hyb\\STOCKDATA.db'
    dbcn = sqlite3.connect(dbfn)
    
    
    j=2818
    for i in range(j,len(gpdmb)):
        gpdm=gpdmb.index[i]
        gpmc = gpdmb.iloc[i]['gpmc']
        print("共有%d只股票，正在处理第%d只：%s%s，请等待…………" % (len(gpdmb),i+1,gpdm,gpmc)) 
#        data = get_ltg(browser,gpdm)
#        
#        if len(data)>0 :
#            dbcn.executemany('''INSERT OR REPLACE INTO LTG (GPDM,RQ,LTG,SDGDCG,SDGDZB,SHCG)
#            VALUES (?,?,?,?,?,?)''', data)

        data1=get_ltg1(browser,gpdm)

        if len(data1)>0 :
            dbcn.executemany('''INSERT OR REPLACE INTO LTG (GPDM,RQ,LTG,SDGDCG,SDGDZB,SHCG)
            VALUES (?,?,?,?,?,?)''', data1)

#        if len(data)==0 and len(data1)==0:
#
#            print("%s%s，读取失败。" % (gpdm,gpmc)) 
                
        if (i % 10 ==0) or i>=len(gpdmb)-1 :
            dbcn.commit()

    dbcn.close()

    browser.quit()        


    now2 = datetime.datetime.now().strftime('%H:%M:%S')
    print('开始运行时间：%s' % now1)
    print('结束运行时间：%s' % now2)

