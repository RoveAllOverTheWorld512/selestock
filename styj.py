# -*- coding: utf-8 -*-
"""

ST股票研究

"""

import sys
import datetime
import pandas as pd
import tushare as ts
import dateutil.parser

##########################################################################
#所有A股列表，包括已退市的
##########################################################################
def get_stklst():
    
#    mytoken='18fcea168f6c1f8621c13bef376e726cf5e31fde3f579db37929181b'
#    pro = ts.pro_api(token=mytoken)
    
    # list_status:L上市 D退市 P暂停上市
    dt0 = pro.stock_basic(list_status='L',fields='ts_code,symbol,name,area,industry,list_date')
    dt0=dt0.set_index('ts_code',drop=False)
    
    dt1 = pro.stock_basic(list_status='P',fields='ts_code,symbol,name,area,industry,list_date')
    dt1=dt1.set_index('ts_code',drop=False)
    
    dt2 = pro.stock_basic(list_status='D',fields='ts_code,symbol,name,area,industry,list_date')
    dt2=dt2.set_index('ts_code',drop=False)
    
    dt=pd.concat([dt0,dt1,dt2])
        
    dt=dt[~dt.index.duplicated()]
    dt=dt.sort_index()

    return dt[['ts_code','name']]


##########################################################################
#2019年被ST股票列表
##########################################################################
def get_st_lst(d1=None,d2=None):

    td = datetime.datetime.now()  #今天
    
    if isinstance(d1,int):
        d1=str(d1)

    if isinstance(d2,int):
        d2=str(d2)

    try:
        d1=dateutil.parser.parse(d1).strftime('%Y-%m-%d')
        d2=dateutil.parser.parse(d2).strftime('%Y-%m-%d')
    except:
        d1=None
        d2=None
        
    if d1==None and d2==None:
        d1 = str(td.year)+'-01-01'
        d2 = str(td.year)+'-12-31'
        
    if d1>d2:
        d1,d2=d2,d1
        
    df = pd.read_csv(r'd:\selestock\stgp.csv',encoding='GBK',dtype='object')
    
    df = df.loc[((df['rq']>=d1) & (df['rq']<=d2))]

    df = df.sort_values(by=['gpdm','rq'])
    
    df1 = df.drop_duplicates(['gpdm','gpmc'],keep='first')
    df2 = df.drop_duplicates(['gpdm','gpmc'],keep='last')

    df = df1.append(df2)
    df = df.sort_values(by=['gpdm','rq'])
    
    df.to_csv(r'd:\selestock\stgpmd.csv',encoding='GBK',index=False)

    df = df[['gpdm']]

#    去重
    df=df[~df['gpdm'].duplicated()]

    return df.gpdm.tolist()

##########################################################################
#股票改名
#改名原因：（15种）
#    其他
#    未股改加S
#    完成股改
#    改名
#    摘G
#    ST
#    撤销ST
#    *ST
#    摘星
#    撤销*ST
#    摘星改名
#    暂停上市
#    恢复上市加N
#    终止上市
#    恢复上市
#
##########################################################################
def get_stknm(gpdm):

    '''
    获取股票改名
    '''        
    mytoken='18fcea168f6c1f8621c13bef376e726cf5e31fde3f579db37929181b'
    pro = ts.pro_api(token=mytoken)
    df = pro.namechange(ts_code=gpdm, fields='ts_code,name,start_date,end_date,change_reason')

    #排序、去重
    df=df.sort_values(by=['start_date','end_date'])
    df = df.drop_duplicates(['start_date'],keep='first')
    
    return df

##########################################################################
#复权股价
##########################################################################
def get_fqgj(gpdm):

    '''
    获取指定
    '''
#    gpdm='600198.SH'
    
    #每日交易信息，注意，如果不输入起始截止日期则获取最近4000个交易日数据，最多获取4000个交易日数据
#    df = pro.daily(ts_code=gpdm,start_date='20010101',end_date='20191231')

    df1 = pro.daily(ts_code=gpdm,start_date='19900101',end_date='20001231')
    df2 = pro.daily(ts_code=gpdm,start_date='20010101',end_date='20101231')
    df3 = pro.daily(ts_code=gpdm,start_date='20110101',end_date='20201231')

    #合并、排序
    df=pd.concat([df1,df2,df3])
    df=df.sort_values(by='trade_date')
    
    #重新索引
    df=df.reset_index(drop=True)
    
    #获取复权因子
    fqyz1 = pro.adj_factor(ts_code=gpdm,start_date='19900101',end_date='20001231')
    fqyz2 = pro.adj_factor(ts_code=gpdm,start_date='20010101',end_date='20101231')
    fqyz3 = pro.adj_factor(ts_code=gpdm,start_date='20110101',end_date='20201231')

    fqyz=pd.concat([fqyz1,fqyz2,fqyz3])
    fqyz=fqyz.sort_values(by='trade_date')
    
    fqgj=pd.merge(df, fqyz, on='trade_date',suffixes=('_x', '_y'))
    
    fqgj=fqgj[['trade_date', 'close','adj_factor']]
    
    fqgj=fqgj.sort_values(by='trade_date',ascending=False)
    
    #最新因子
    zxyz=fqgj.iloc[0].adj_factor
    #前复权
    fqgj=fqgj.assign(close_adj=fqgj['close']*(fqgj['adj_factor']/zxyz))

    fqgj=fqgj.sort_values(by='trade_date')
    
    

    return fqgj

##########################################################################
#前复权因子
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
#涨幅
##########################################################################
def get_zf(rq1,rq2):
    
    gj1 = pro.daily(trade_date=rq1)
    gj2 = pro.daily(trade_date=rq2)
    gj = pd.merge(gj2, gj1, on='ts_code',suffixes=('_x', '_y'))
    
    gj = gj[['ts_code', 'trade_date_x', 'close_x','trade_date_y', 'close_y']]

    zf = gj.assign(zdf=gj['close_x']/gj['close_y'])
    return zf[['ts_code','zdf']]
    

##########################################################################
#股票改名
##########################################################################
def get_stknms():
    gpdmb=get_stklst()
    dt=get_stknm(gpdmb.iloc[0].ts_code)
    for i in range(1,len(gpdmb)):
        gpdm=gpdmb.iloc[i].ts_code
        
        print(gpdm)
        df=get_stknm(gpdm)
        dt=dt.append(df)

    dt = dt.sort_values(by=['ts_code','start_date']) 
    
    dt.to_csv(r'd:\selestock\gpgm.csv',encoding='GBK',index=False)


##########################################################################
#最大回撤
##########################################################################
def max_drawdown(df):
    return ((df/df.expanding(min_periods=1).max()).min()-1)*100


##########################################################################
#最大下跌
##########################################################################
def max_downside(gpdm,rq1,rq2):
    
    td=dateutil.parser.parse(rq1)

    m3=(td+datetime.timedelta(30*3)).strftime("%Y%m%d")
    m6=(td+datetime.timedelta(30*6)).strftime("%Y%m%d")
    m9=(td+datetime.timedelta(30*9)).strftime("%Y%m%d")
    m12=(td+datetime.timedelta(365)).strftime("%Y%m%d")
    m24=(td+datetime.timedelta(365*2)).strftime("%Y%m%d")


    gjdf=get_fqgj(gpdm)
    
    #将series转换成list，再定位日期的前一个交易日日
    dd=gjdf['trade_date'].tolist()
    
    for i in range(len(dd)):
        if dd[i]>=rq1:
            break
    ix=i
        
#    ix=dd.index(rq1)

    if ix>1:
        rq0=dd[ix-1]
    else:
        rq0=rq1
    
    data=[rq0,rq1]      #起始日期
    
    for rq in [rq2,m3,m6,m9,m12,m24]:
        data.append(rq)     #截止日期
        if rq<=rq2:

            df=gjdf.loc[(gjdf['trade_date']>=rq0) & (gjdf['trade_date']<rq),['trade_date','close_adj']]
            
            #最低价
            mn=df['close_adj'].min()

            #最低价索引值
            mnidx=df['close_adj'].idxmin()

            #最低价日期
            mndt=df.loc[mnidx]['trade_date']

            #最低跌幅
            mxdf=(mn/df['close_adj'].iloc[0]-1)*100
            mxdf=round(mxdf,2)
            
            dd=dateutil.parser.parse(mndt)-dateutil.parser.parse(rq0)
            
            data.append(mndt)       #低点日期
            data.append(dd.days)    #天数
            data.append(mxdf)       #跌幅

        else:

            data.append(None)
            data.append(None)
            data.append(None)
            
    return data

##########################################################################
#ST股票日期
##########################################################################
def get_st_date():

    #获取退市时间
    dt2 = pro.stock_basic(list_status='D',fields='ts_code,delist_date')    
    tsgdict = {v[0]:v[1] for i,v in dt2.iterrows()}
    
    td = datetime.datetime.now().strftime("%Y%m%d") #今天

    dt = pd.read_csv(r'd:\selestock\gpgm.csv',encoding='GBK',dtype='object')
    dt = dt.sort_values(by=['ts_code','start_date']) 

    #提取股票名称包含ST
    dt1=dt[dt['name'].str.contains('ST')]    
    dt1=dt1[['ts_code']]

    #去重
    dt1=dt1[~dt1['ts_code'].duplicated()]
    #列表
    dt1=dt1['ts_code'].tolist()
    
    data=pd.DataFrame(columns=['gpdm','gpmc'])

    for i in range(len(dt1)):
        gpdm=dt1[i]
        
        print("共有%d只股票，正在处理第%d只：%s" % (len(dt1),i+1,gpdm))
        
        dt2=dt.loc[dt['ts_code']==gpdm]
        
        for idx,v in dt2.iterrows():
            if 'ST' in v[1]:
                gpmc=v[1]
                d1=v[2]
                #如果没有截止日期：退市股用退市日期，上市股用今天日期
                if pd.isnull(v[3]):
                    if v[0] in tsgdict.keys():
                        d2=tsgdict[v[0]]
                    else:
                        d2=td
                else:
                    d2=v[3]
                    
                index=pd.date_range(d1,d2,freq='B')
    
                df=pd.DataFrame(data={'gpdm':gpdm,'gpmc':gpmc},index=index)
                
                data=data.append(df)

        
        data.index.name='rq'
    
        data.to_csv(r'd:\selestock\stgp.csv',encoding='GBK')
                    
    return
            

    

if __name__ == '__main__':
    mytoken='18fcea168f6c1f8621c13bef376e726cf5e31fde3f579db37929181b'
    pro = ts.pro_api(token=mytoken)
    
#   生成股票改名信息，保存为d:\selestock\gpgm.csv
#    get_stknms()
    
#   生成ST股票日期，保存为d:\selestock\stgp.csv
#    get_st_date()


#    sys.exit()

    

    bg = '20100101'
    
    dt = pd.read_csv(r'd:\selestock\gpgm.csv',encoding='GBK',dtype='object')
    dt = dt.sort_values(by=['ts_code','start_date']) 
    
    #改名日期在2010年01月01日以后
    dt=dt[dt['start_date']>bg]
    
    #提取股票名称包含ST
    dt1=dt[dt['name'].str.contains('ST')]

    #改名原因为“ST”或“*ST”
    dt1=dt1[(dt1['change_reason']=='*ST') | (dt1['change_reason']=='ST')]
    
    dt1=dt1[['ts_code']]

    dt1=dt1[~dt1['ts_code'].duplicated()]
    gplb=dt1.ts_code.tolist()
#    sys.exit()
    
    data=[]
    j=0
    for i in range(j,len(gplb)):
        gpdm=gplb[i]
        dt2=dt.loc[((dt['ts_code']==gpdm) & (dt['name'].str.contains('ST')))]
        
        for l,v in dt2.iterrows():
            gpmc=v[1]
            rq1=v[2]
            rq2=v[3]
            if pd.isnull(rq2):
                rq2='20191231'
            
            print(i,gpdm,gpmc,rq1,rq2)
            
            ret=[gpdm,gpmc]    
            mxds=max_downside(gpdm,rq1,rq2)    
            ret.extend(mxds)

            data.append(ret)

    cols='gpdm,gpmc,rq0,rq1,rq2,rq_l,days,max_df,m3,m3_l,m3_days,m3_df,'+ \
         'm6,m6_l,m6_days,m6_df,m9,m9_l,m9_days,m9_df,'+ \
         'm12,m12_l,m12_days,m12_df,m24,m24_l,m24_days,m24_df'
    
    cols=cols.split(',')

    df=pd.DataFrame(data,columns=cols)
    
    df.to_csv(r'd:\selestock\styj.csv',encoding='GBK',index=False)
        
    sys.exit()



    #股票代码去重    
#    dt2=dt1[~dt1['ts_code'].duplicated()]
#    
#    dt0 = pro.stock_basic(list_status='L',fields='ts_code,symbol,name,area,industry,list_date')
#    dt0=dt0.set_index('ts_code',drop=False)
#    
#    #正在上市
#    dt3=dt2[dt2['ts_code'].isin(dt0.index)]
#
#    #已经退市
#    dt4=dt2[~dt2['ts_code'].isin(dt0.index)]
#    
#    dt5=dt[dt['ts_code'].isin(dt0.index)]
#    dt5=dt5.sort_values(by='start_date',ascending=False) 
#    dt5=dt5.set_index('ts_code',drop=False)
#    dt5=dt5[~dt5.index.duplicated(keep='first')]
#    dt5=dt5[dt5.index.isin(dt3['ts_code'])]
#    
#    fn = r'd:\selestock\ST股票汇总.xlsx'
#
#    if os.path.exists(fn):
#        os.remove(fn)
#
#    writer=pd.ExcelWriter(fn,engine='xlsxwriter')
#
#    dt3.to_excel(writer, sheet_name='正在上市',index=False)   
#    dt5.to_excel(writer, sheet_name='当前名称',index=False)  
#    dt4.to_excel(writer, sheet_name='已经退市',index=False)  
#
#    writer.save()

    rq1='20180601'
    rq2='20181214'

    zf=get_zf(rq1,rq2)
    qfq=get_qfqyz(rq1,rq2)
    qfqzf=pd.merge(zf,qfq,on='ts_code')
    qfqzf=qfqzf.assign(zf=qfqzf['zdf']/qfqzf['qfq']*100.00-100.00)
   
    
    dt1=dt[dt['start_date']<rq1]
    dt1=dt1.sort_values(by='start_date',ascending=False)
    dt2=dt1[~dt1['ts_code'].duplicated(keep='first')]
    
    dt0 = pro.stock_basic(list_status='L',fields='ts_code,symbol,name,area,industry,list_date')
    dt0=dt0.set_index('ts_code',drop=False)
    
    #正在上市
    dt3=dt2[dt2['ts_code'].isin(dt0.index)]
    dt4=dt3[dt3['name'].str.contains('ST')]
    
    #前复权因子
    adj1 = pro.adj_factor(ts_code='', trade_date=rq1)
    adj2 = pro.adj_factor(ts_code='', trade_date=rq2)
    adj = pd.merge(adj2, adj1, on='ts_code',suffixes=('_x', '_y'))
    adj = adj.assign(qfq = adj['adj_factor_y'] / adj['adj_factor_x'])
    adj = adj[['ts_code','qfq']]


    gj1 = pro.daily(trade_date=rq1)
    gj2 = pro.daily(trade_date=rq2)
    gj = pd.merge(gj2, gj1, on='ts_code',suffixes=('_x', '_y'))
    
    gj = gj[['ts_code', 'trade_date_x', 'close_x','trade_date_y', 'close_y']]
    
    zf= pd.merge(gj, adj, on='ts_code',suffixes=('_x', '_y'))

    zf = zf.assign(zdf=zf['close_x']/(zf['close_y']*zf['qfq'])*100.00-100.00)

    stzf=pd.merge(dt4, zf, on='ts_code',suffixes=('_x', '_y'))
    
    stzf.to_excel(r'd:\selestock\2011stzf.xlsx')
    
