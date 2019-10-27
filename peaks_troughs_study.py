# -*- coding: utf-8 -*-
"""
Created on Thu Mar  7 16:46:14 2019

@author: lenovo

给定一个数组h, 从左往右扫描。 

用S记录当前的状态（未知0， 下坡1，上坡2) 

当S=0, 如果 h[i] >  h[i+1]  修改状态为 下坡1， 否则为上坡 2 

当S=1, 如果 h[i]  <  h[i+1], 则判断为由下坡变为上坡， 此处为一个波谷。 
    如果该波谷比上一个rangesize范围内的波谷更低，则修改上一个波谷的值。 
    否则就将该波谷加入波谷列表。 

当S=2, 如果 h[i]  > h[i+1], 则判断为由上坡变为下坡， 此处为一个波峰。 
    如果该波峰比上一个rangesize范围内的波峰更高，则修改上一个波谷的值。 
    否则就将该波峰加入波峰列表。 

最后返回波峰和波谷列表。 
--------------------- 
作者：swordboris 
来源：CSDN 
原文：https://blog.csdn.net/boris_wang2008/article/details/81209849 
版权声明：本文为博主原创文章，转载请附上博文链接！
"""


import pandas as pd
from matplotlib import pyplot as plt
import sys
import selestock as my

def get_peaks_troughs(ds):
    '''
    输入参数：ds为pandas.Series,index为时间
    '''
    
    ds.index=pd.to_datetime(ds.index)
    ds.sort_index(inplace=True)     #按日期排序
    
    #波峰
    peaks = dict()
    #波谷
    troughs = dict()
    
    S = 0
    for x in range(len(ds)-1):
        idx=ds.index[x]
        idx1=ds.index[x+1]
        if S == 0:
            if ds[idx] > ds[idx1]:
                S = 1 ## down
                peaks[idx]=ds[idx]
            else:
                S = 2 ## up
                troughs[idx]=ds[idx]                           
                
        elif S == 1:
            if ds[idx] < ds[idx1]:
                S = 2
                ## from down to up
                troughs[idx]=ds[idx]                           

        elif S == 2:
            if ds[idx] > ds[idx1]:
                S = 1
                ## from up to down
                peaks[idx]=ds[idx]

    ##end point                
    x=len(ds)-1       
    if S==1:
        troughs[idx]=ds[idx]                           
    else :
        peaks[idx]=ds[idx]
    
    peaks=pd.Series(peaks)
    troughs=pd.Series(troughs)
    
    return peaks,troughs



def best_peaks_troughs(peaks,troughs,pv,tv):
    '''
    pv:有效涨幅阈值，如0.2表示上涨幅度超过20%
    tv:有效回撤阈值，如0.2表示回撤幅度大于20%
    '''
#    pv=0.2
#    tv=0.2

    #去掉上升中继和下跌中继
    pt=[]
    for i in peaks.index:
        pt.append((i,peaks[i]))
        
    for i in troughs.index:
        pt.append((i,troughs[i]))
        
    pt.sort(key=lambda x:x[0])

    start=pt[0]
    end=pt[-1]
    
    #去掉上升中继和下跌中继
    i=2
    while i<len(pt)-1 and len(pt)>4:

        if i<2:
            i=2
            
        p0=pt[i-2]
        p1=pt[i-1]
        p2=pt[i]
        p3=pt[i+1]
#        print(p0[0],p1[0],p2[0],p3[0])
#        print(p0[1],p1[1],p2[1],p3[1])

        if (p1[1]>p0[1] and p2[1]<p1[1] and p3[1]>p2[1] 
            and p2[1]>=p0[1] and p3[1]>p1[1]):     #4点N形上升通道情形一

#            print('A')
            zf = abs(p2[1]/p1[1]-1)     #回调幅度

            if (zf<tv):
                pt.remove(p1)
                pt.remove(p2)
                i -= 2
            else:
                i += 1
        elif (p1[1]>p0[1] and p2[1]<p1[1] and p3[1]>p2[1] 
            and p2[1]>=p0[1] and p3[1]<=p1[1]):     #4点N形上升通道情形二

#            print('B')
            zf = abs(p2[1]/p1[1]-1)     #回调幅度

            if (zf<tv):
                pt.remove(p2)
                pt.remove(p3)
                i -= 2
            else:
                i += 1
                
        elif (p1[1]<p0[1] and p2[1]>p1[1] and p3[1]<p2[1] 
            and p2[1]<=p0[1] and p3[1]<p1[1]):     #4点倒N形下降通道情形一

#            print('C')
            zf = abs(p2[1]/p1[1]-1)     #上升幅度

            if (zf<pv):
                pt.remove(p1)
                pt.remove(p2)
                i -= 2
            else:
                i += 1
        
        elif (p1[1]<p0[1] and p2[1]>p1[1] and p3[1]<p2[1] 
            and p2[1]<=p0[1] and p3[1]>=p1[1]):     #4点倒N形下降通道情形二

#            print('D')
            zf = abs(p2[1]/p1[1]-1)     #上升幅度
            if (zf<pv):
                pt.remove(p2)
                pt.remove(p3)
                i -= 2
            else:
                i += 1
                
        #由于是成对删除的，应该不会出现以下这两种情况        
        elif (p1[1]>p0[1] and p2[1]>p1[1]): #3点“丿”形上升中继 
#            print(p0[0],p1[0],p2[0],p3[0])
#            print(p0[1],p1[1],p2[1],p3[1])
#            print('E')
            pt.remove(p1)
            i -= 1

        elif (p1[1]>p0[1] and p2[1]>p1[1]): #3点“L”形下降中继 
#            print(p0[0],p1[0],p2[0],p3[0])
#            print(p0[1],p1[1],p2[1],p3[1])
#            print('F')
            pt.remove(p1)
            i -= 1

        else:
#            print(p0[0],p1[0],p2[0],p3[0])
#            print(p0[1],p1[1],p2[1],p3[1])
#            print('G')
            i += 1
    
    pt=list(set([start]+pt+[end]))
    pt.sort(key=lambda x:x[0])
                
            
    pout=list()
    tout=list()

    p1 = pt[0]
    for i in range(1,len(pt)):
#        print(i,pt[i])
        p2=pt[i]

        if p2[1]>p1[1]:
            tout.append(p1)
            fg=0
        else:
            pout.append(p1)
            fg=1

        p1=p2

    if fg==0:
        pout.append(p2)
    else:
        tout.append(p2)
        
    pout=pd.DataFrame(pout,columns=['date','price'])
    pout=pout.set_index('date')
    pout=pout['price']

    tout=pd.DataFrame(tout,columns=['date','price'])
    tout=tout.set_index('date')
    tout=tout['price']
        
        
    return pout,tout

def max_up(peaks,troughs):
    tt=pp=zf=None
    
    for t in troughs.index:
        for p in peaks.index:
            if p>t:
                z=(peaks[p]/troughs[t]-1)*100
                if (zf is None) or zf<z:
                    zf=z
                    tt=t
                    pp=p
                    
                
    
    return tt.strftime('%Y-%m-%d'),pp.strftime('%Y-%m-%d'),round(zf,2)
        


if __name__ == "__main__":
#
#    sys.exit()
    
#    df = pd.read_csv(r'd:\selestock\sz300349.csv', dtype={'date':'object'})
#    df = pd.read_csv(r'd:\selestock\tmp.csv', dtype={'date':'object'})
#    df = df.set_index('date')
#    ds = df['price']

    df = my.get_fqgj('002456.SZ','20150101','20181231')
    ds = round(df['adj_close'],4)
#    peaks,troughs = get_peaks_troughs(ds)
    p=(ds.diff(1) > 0) & (ds.diff(-1) >= 0)
    peaks=ds.loc[p]
    t=(ds.diff(1) < 0) & (ds.diff(-1) <= 0)
    troughs=ds.loc[t]
    peaks_troughs = peaks.append(troughs)
    start = pd.Series([ds[0]], index=[ds.index[0]])
    end = pd.Series([ds[-1]], index=[ds.index[-1]])
    peaks_troughs = peaks_troughs.append(start)
    peaks_troughs = peaks_troughs.append(end)
    
    
    fig = plt.figure(figsize=(50,10))
    plt.plot(ds)

    peaks,troughs = best_peaks_troughs(peaks,troughs,0.20,0.20)

    print(max_up(peaks,troughs))

    for x in peaks.index:
        y=peaks[x]
        plt.text(x,y,y,fontsize=10,verticalalignment="bottom",horizontalalignment="center")
    for x in troughs.index:
        y=troughs[x]
        plt.text(x,y,y,fontsize=10,verticalalignment="top",horizontalalignment="center")

    plt.show()

