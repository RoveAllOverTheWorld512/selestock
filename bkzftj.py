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
import numpy as np
import pandas as pd
import struct
import winreg
import tushare as ts
import dateutil.parser
import xlrd
import pypinyin


########################################################################
#初始化本程序配置文件
########################################################################
def hz2py(word):
#    return pypinyin.slug(word, heteronym=True, style=pypinyin.FIRST_LETTER).replace('-','').upper()
    return pypinyin.slug(word, style=pypinyin.FIRST_LETTER).replace('-','').upper()



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
def zzhy():

    files = os.listdir(getdrive()+'\\syl')
    fs = [re.findall('csi(\d{8})\.xls',e) for e in files]
    jyrlist =[]
    for e in fs:
        if len(e)>0:
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
    
    return df

#############################################################################
#读取证监会行业分类
#############################################################################
def zjhhy():

    files = os.listdir(getdrive()+'\\pe')
    fs = [re.findall('(\d{8})\.xls',e) for e in files]
    jyrlist =[]
    for e in fs:
        if len(e)>0:
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
    
    return df

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
#计算涨幅
##########################################################################
def get_zf(rq1,rq2):
    
    gj1 = pro.daily(trade_date=rq1)
    gj2 = pro.daily(trade_date=rq2)
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
    if rq<'20000101' :
        print('日期必须大于20000101')
        return None

    #获取交易日历2000年以后    
    cal=pro.trade_cal(start_date='20000101',fields='cal_date,is_open,pretrade_date')
    
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
    
if __name__ == '__main__':
    
#    sys.exit()
    
    #tushare 通过Python SDK 调取数据
    #https://tushare.pro/document/1?doc_id=131
    
#    【Python】使用python实现汉字转拼音（2018.12更新）
#    https://blog.csdn.net/mydistance/article/details/85009791
    
    mytoken='18fcea168f6c1f8621c13bef376e726cf5e31fde3f579db37929181b'
    pro = ts.pro_api(token=mytoken)

    rq1 = get_tradedate('20190601')
    rq2 = get_tradedate('20190613')
    

    qfqzf=get_zf(rq1,rq2)

    
    blks=gettdxblk('gn')
#    sys.exit()
    blks['ST板块']=get_stbk()
    blks['次新股']=get_cxgbk()
#    sys.exit()
    
    blkzf=[]
    
    for blk in blks.values():
        blkstk=[lgpdm(gpdm) for gpdm in blk[2]]

        if blk[0]=='次新股':
            blkggzf=qfqzf[qfqzf['ts_code'].isin(blkstk)]
        else:
            blkggzf=qfqzf[(qfqzf['ts_code'].isin(blkstk) & ~qfqzf['ts_code'].isin(blks['次新股'][2]))]

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
            
            blkzf.append([blk[0],n,blkpjzf,hdm,hmc,h,ldm,lmc,l,gpdms])
    
    cols=['blkname','num','pjzf','max_dm','max_mc','max_zf','min_dm','min_mc','min_zf','gpdms']
    df=pd.DataFrame(blkzf,columns=cols)

    df=df.sort_values(by='pjzf',ascending=False)
    

    h=qfqzf['zf'].max()
    i=qfqzf[(qfqzf['zf']==h)].index[0]
    
    hdm=qfqzf['ts_code'][i]
    hmc=qfqzf['name'][i]
    
    l=qfqzf['zf'].min()
    i=qfqzf[(qfqzf['zf']==l)].index[0]
    ldm=qfqzf['ts_code'][i]
    lmc=qfqzf['name'][i]

    pjzf=qfqzf['zf'].mean()

    allzf=[['全部股票',len(qfqzf),pjzf,hdm,hmc,h,ldm,lmc,l,'']]
    cols=['blkname','num','pjzf','max_dm','max_mc','max_zf','min_dm','min_mc','min_zf','gpdms']
    df1=pd.DataFrame(allzf,columns=cols)

    df=df.append(df1)

    #加拼音
    df['blkpy']=df['blkname'].map(hz2py)
    cols=['blkname','blkpy','num','pjzf','max_dm','max_mc','max_zf','min_dm','min_mc','min_zf','gpdms']
    df=df[cols]
    
    gndf=df.round(2)

   
    tdxsw=tdxswhy()
    tdxsw['tdx_hypy']=tdxsw['tdx_hymc'].map(hz2py)
    tdxsw['sw_hypy']=tdxsw['sw_hymc'].map(hz2py)
    
    hybk=pd.merge(qfqzf,tdxsw,on='ts_code')
    grouped=hybk.groupby(['tdx_hydm','tdx_hymc','tdx_hypy'])
    tdxhybk=grouped['zf'].agg([np.size, np.mean, np.std, np.max, np.min])
    tdxhybk=tdxhybk.round(2)
    tdxhybk=tdxhybk.sort_values(by='mean',ascending=False)
    
    grouped=hybk.groupby(['sw_hydm','sw_hymc','sw_hypy'])
    swhybk=grouped['zf'].agg([np.size, np.mean, np.std, np.max, np.min])
    swhybk=swhybk.round(2)
    swhybk=swhybk.sort_values(by='mean',ascending=False)
    
    
    headdf=qfqzf.head(200).copy()
    headdf=headdf[['ts_code','name','zf','list_date']]
    
    taildf=qfqzf.tail(200).copy()
    taildf=taildf[['ts_code','name','zf','list_date']]
    taildf=taildf.sort_values(by='zf',ascending=True)
    
    
    fn=r'd:\selestock\板块涨幅_%s_%s.xlsx' % (rq1,rq2)

    writer=pd.ExcelWriter(fn,engine='xlsxwriter')

    gndf.to_excel(writer, sheet_name='概念板块',index=False)   
    tdxhybk.to_excel(writer, sheet_name='通达信行业板块')   
    swhybk.to_excel(writer, sheet_name='申万行业板块')   
    headdf.to_excel(writer, sheet_name='涨幅最大个股',index=False)   
    taildf.to_excel(writer, sheet_name='跌幅最大个股',index=False) 
    qfqzf.to_excel(writer, sheet_name='全部个股',index=False) 
    
#    dt5.to_excel(writer, sheet_name='当前名称',index=False)  
#    dt4.to_excel(writer, sheet_name='已经退市',index=False)  
#
#    df.to_excel(fn,index=False)

    writer.save()

#    dt1=dt[dt['start_date']<rq1]
#    dt1=dt1.sort_values(by='start_date',ascending=False)
#    dt2=dt1[~dt1['ts_code'].duplicated(keep='first')]
#    
#    dt0 = pro.stock_basic(list_status='L',fields='ts_code,symbol,name,area,industry,list_date')
#    dt0=dt0.set_index('ts_code',drop=False)
#    
#    #正在上市
#    dt3=dt2[dt2['ts_code'].isin(dt0.index)]
#    dt4=dt3[dt3['name'].str.contains('ST')]
#    
#    #前复权因子
#    adj1 = pro.adj_factor(ts_code='', trade_date=rq1)
#    adj2 = pro.adj_factor(ts_code='', trade_date=rq2)
#    adj = pd.merge(adj2, adj1, on='ts_code',suffixes=('_x', '_y'))
#    adj = adj.assign(qfq = adj['adj_factor_y'] / adj['adj_factor_x'])
#    adj = adj[['ts_code','qfq']]
#
#
#    gj1 = pro.daily(trade_date=rq1)
#    gj2 = pro.daily(trade_date=rq2)
#    gj = pd.merge(gj2, gj1, on='ts_code',suffixes=('_x', '_y'))
#    
#    gj = gj[['ts_code', 'trade_date_x', 'close_x','trade_date_y', 'close_y']]
#    
#    zf= pd.merge(gj, adj, on='ts_code',suffixes=('_x', '_y'))
#
#    zf = zf.assign(zdf=zf['close_x']/(zf['close_y']*zf['qfq'])*100.00-100.00)
#
#    stzf=pd.merge(dt4, zf, on='ts_code',suffixes=('_x', '_y'))
#    
#    stzf.to_excel(r'd:\selestock\2011stzf.xlsx')
#    
