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

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt


def get_peaks_troughs(h):
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


def best_peaks_troughs(peaks,troughs,tv):

    #计算波峰、波谷满足涨跌幅的情况
    tv=12
    pout=list()
    tout=list()
    for i in range(len(peaks)):
        p=peaks[i]
        for j in range(len(troughs)):
            t=troughs[j]
            if p[0]<t[0]:
                zf=round((t[1]/p[1]-1)*100,2)
                if zf<-tv:
                    pout.append([p,t,zf])
                    
            if p[0]>t[0]:
                zf=round((p[1]/t[1]-1)*100,2)
                if zf>tv:
                    tout.append([t,p,zf])
     
    #筛选满足跌幅要求的波峰
    plist=list(set([e[0] for e in pout]))
    plist.sort(key=lambda x:x[0])

    #筛选波谷涨幅组合中
    t=[]
    for e in tout:
        if e[1] in plist:
            t.append(e)

    #筛选满足涨幅要求的波谷
    tlist=list(set([e[1] for e in t]))
    tlist.sort(key=lambda x:x[0])
    
    p=[]
    for e in pout:
        if e[0] in tlist:
            p.append(e)
    
    p.sort(key=lambda x:x[0])           
    t.sort(key=lambda x:x[0])       


    return p,t



if __name__ == "__main__":

    df = pd.read_csv(r'd:\selestock\sz000069.csv', dtype={'date':'object'})
    h=df['price'].tolist()
    fig = plt.figure(figsize=(30,20))
    plt.plot(np.arange(len(h)),h)

    peaks,troughs = get_peaks_troughs(h)

    for x,y in peaks:
        plt.text(x,y,y,fontsize=10,verticalalignment="bottom",horizontalalignment="center")
    for x,y in troughs:
        plt.text(x,y,y,fontsize=10,verticalalignment="top",horizontalalignment="center")

    plt.show()

