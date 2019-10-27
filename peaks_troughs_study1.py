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

import sys
import cv2 as cv
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt


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

def get_peaks_troughs1(ds):
    '''
    输入参数：ds为pandas.Series,index为时间
    '''
    
    ds.index=pd.to_datetime(ds.index)
    ds.sort_index(inplace=True)     #按日期排序
    
    #波峰
    peaks = list()
    #波谷
    troughs = list()
    
    S = 0
    for x in range(len(ds)-1):
        idx=ds.index[x]
        idx1=ds.index[x+1]
        if S == 0:
            if ds[idx] > ds[idx1]:
                S = 1 ## down
                peaks.append((idx,ds[idx]))
            else:
                S = 2 ## up
                troughs.append((idx,ds[idx]))                           
                
        elif S == 1:
            if ds[idx] < ds[idx1]:
                S = 2
                ## from down to up
                troughs.append((idx,ds[idx]))                           

        elif S == 2:
            if ds[idx] > ds[idx1]:
                S = 1
                ## from up to down
                peaks.append((idx,ds[idx]))

    ##end point                
    x=len(ds)-1       
    if S==1:
        troughs.append((idx,ds[idx]))                           
    else :
        peaks.append((idx,ds[idx]))
        
    return peaks,troughs


def get_peaks_troughs0(h):
    #波峰
    peaks = list()
    #波谷
    troughs = list()
    S = 0
    for x in range(len(h)-1):
        if S == 0:
            if h[x] > h[x+1]:
                S = 1 ## down
                peaks.append((x,h[x]))
            else:
                S = 2 ## up
                troughs.append((x,h[x]))                           
                
        elif S == 1:
            if h[x] < h[x+1]:
                S = 2
                ## from down to up
                troughs.append((x,h[x]))                           

        elif S == 2:
            if h[x] > h[x+1]:
                S = 1
                ## from up to down
                peaks.append((x,h[x]))

    ##end point                
    x=len(h)-1       
    if S==1:
        troughs.append((x,h[x]))                           
    else :
        peaks.append((x,h[x]))
        
    return peaks,troughs


def best_peaks_troughs0(peaks,troughs,pv,tv):
    '''
    pv:有效涨幅阈值，如0.2表示上涨幅度超过20%
    tv:有效回撤阈值，如0.2表示回撤幅度大于20%
    '''
    pv=0.2
    tv=0.2
    peaks.sort(key=lambda x:x[0])
    troughs.sort(key=lambda x:x[0])
    '''
    针对每个峰值，计算前面每股谷值到该峰值的涨幅，大于阈值就添加到ps列表
    '''
    ps=[]
    for i in range(len(peaks)):
        p=peaks[i]
        for j in range(len(troughs)):
            t=troughs[j]
            #回调过程
            if t[0]>p[0] and p[1]*(1-tv)>t[1]:
                ps.append([p,t])
                
            #上涨过程
            if p[0]>t[0] and t[1]*(1+pv)<p[1]:
                ps.append([t,p])
     
    ps=list(set([e1 for e1,e2 in ps]+[e2 for e1,e2 in ps]))           

    ps.sort(key=lambda x:x[0])

    pt = peaks+troughs
    pt.sort(key=lambda x:x[0])

    ps=ps+[pt[0],pt[-1]]
    ps=list(set(ps))
    ps.sort(key=lambda x:x[0])

    pt=ps
    
    pout=list()
    tout=list()
    p1 = pt[0]
    for i in range(1,len(pt)):
        p2=pt[i]
        if p2[1]>p1[1]:
            tout.append(p1)
        else:
            pout.append(p1)
        p1=pt[i]
        
    print(pt)    
    return pout,tout



def best_peaks_troughs(peaks,troughs,pv,tv):
    '''
    pv:有效涨幅阈值，如0.2表示上涨幅度超过20%
    tv:有效回撤阈值，如0.2表示回撤幅度大于20%
    '''
#    pv=0.2
#    tv=0.2

    #去掉上升中继和下跌中继
    
    pt=list()
    for i in peaks.index:
        pt.append((i,peaks[i]))
        
    for i in troughs.index:
        pt.append((i,troughs[i]))
        
        
    
    pt.sort(key=lambda x:x[0])
    
    start=pt[0]
    end=pt[-1]
    
    i=2
    while i<len(pt)-1 and len(pt)>4:

        if i<2:
            i=2
            
        p0=pt[i-2]
        p1=pt[i-1]
        p2=pt[i]
        p3=pt[i+1]
        print(p0[0],p1[0],p2[0],p3[0])
        print(p0[1],p1[1],p2[1],p3[1])

        if (p1[1]>p0[1] and p2[1]<p1[1] and p3[1]>p2[1] 
            and p2[1]>=p0[1] and p3[1]>p1[1]):     #上升通道

            print('A')
            zf = abs(p2[1]/p1[1]-1)     #回调幅度

            if (zf<tv):
                pt.remove(p1)
                pt.remove(p2)
                i -= 2
            else:
                i += 1
        elif (p1[1]>p0[1] and p2[1]<p1[1] and p3[1]>p2[1] 
            and p2[1]>=p0[1] and p3[1]<=p1[1]):     #上升通道

            print('B')
            zf = abs(p2[1]/p1[1]-1)     #回调幅度

            if (zf<tv):
                pt.remove(p2)
                pt.remove(p3)
                i -= 2
            else:
                i += 1
                
        elif (p1[1]<p0[1] and p2[1]>p1[1] and p3[1]<p2[1] 
            and p2[1]<=p0[1] and p3[1]<p1[1]):     #下降通道

            print('C')
            zf = abs(p2[1]/p1[1]-1)     #上升幅度

            if (zf<pv):
                pt.remove(p1)
                pt.remove(p2)
                i -= 2
            else:
                i += 1
        
        elif (p1[1]<p0[1] and p2[1]>p1[1] and p3[1]<p2[1] 
            and p2[1]<=p0[1] and p3[1]>=p1[1]):     #下降通道

            print('D')
            zf = abs(p2[1]/p1[1]-1)     #上升幅度
            if (zf<pv):
                pt.remove(p2)
                pt.remove(p3)
                i -= 2
            else:
                i += 1
        else:
            print('E')
            i += 1
    
    pt=list(set([start]+pt+[end]))
    pt.sort(key=lambda x:x[0])
                
            
    pout=list()
    tout=list()

    p1 = pt[0]
    for i in range(1,len(pt)):
        print(i,pt[i])
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
        
        
        
    return pout,tout


    

        


if __name__ == "__main__":
#
    df = pd.read_csv(r'd:\selestock\sz300349.csv', dtype={'date':'object'})
#    df = pd.read_csv(r'd:\selestock\tmp.csv', dtype={'date':'object'})
    df = df.set_index('date')
    ds = df['price']
    peaks,troughs = get_peaks_troughs(ds)
    
#    sys.exit()
    h=df['price'].tolist()
    fig = plt.figure(figsize=(50,10))
    plt.plot(ds)

#    peaks,troughs = get_peaks_troughs(h,10)
#    peaks,troughs = get_peaks_troughs(h)

#    peaks,troughs = best_peaks_troughs(peaks,troughs,0.3,0.2)

    for x in peaks.index:
        y=peaks[x]
        plt.text(x,y,y,fontsize=10,verticalalignment="bottom",horizontalalignment="center")
    for x in troughs.index:
        y=troughs[x]
        plt.text(x,y,y,fontsize=10,verticalalignment="top",horizontalalignment="center")


    plt.show()


    sys.exit()
    

    img = cv.imread("peak.png",0)
    img =  255 - img

    #像素行、列
    Y,X = img.shape
    print(Y,X)

    h = np.zeros((X,1))
    for i in range(X):
        for j in range(Y):
            if img[j,i] > 0:
                h[i] = Y - j  

    peaks,troughs = get_peaks_troughs(h,1)

    fig = plt.figure(figsize=(30,20))

    plt.subplot(2,1,1)
    plt.imshow(img)
    plt.subplot(2,1,2)

    plt.plot(np.arange(len(h)),h)
    for x,y in peaks:
        plt.text(x,y,y,fontsize=10,verticalalignment="bottom",horizontalalignment="center")
    for x,y in troughs:
        plt.text(x,y,y,fontsize=10,verticalalignment="top",horizontalalignment="center")

    plt.show()
 