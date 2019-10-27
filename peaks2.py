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
from treelib import Node,Tree


def get_peaks_troughs(h,rangesize):
    peaks = list()
    troughs = list()
    S = 0
    for x in range(1,len(h)-1):
        if S == 0:
            if h[x] > h[x+1]:
                S = 1 ## down
            else:
                S = 2 ## up
                
        elif S == 1:
            if h[x] < h[x+1]:
                S = 2
                ## from down to up
                if len(troughs):
                    ## check if need merge
                    (prev_x,prev_trough) =  troughs[-1]
                    if x - prev_x < rangesize:
                        if prev_trough > h[x]:
                            troughs[-1] = (x,h[x])
                    else:
                        troughs.append((x,h[x])) 
                else:
                    troughs.append((x,h[x]))                           

        elif S == 2:
            if h[x] > h[x+1]:
                S = 1
                ## from up to down
                if len(peaks):
                    prev_x,prev_peak =  peaks[-1]
                    if x - prev_x < rangesize:
                        if prev_peak < h[x]:
                            peaks[-1] = (x,h[x])
                    else:
                        peaks.append((x,h[x]))
                else:
                    peaks.append((x,h[x]))
                
    return peaks,troughs


def get_peaks_troughs1(h):
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

    #去掉上升中继和下跌中继
    pt = peaks+troughs
    pt.sort(key=lambda x:x[0])

#    df=pd.DataFrame(pt,columns=['date','price'])
#    df.to_csv(r'd:\selestock\tmp2.csv')
    #=ABS(C4/C3-1)*100    

#    tv=15
    tv1=tv
    tv2=100*tv/(100-tv)
    
    i=2
    p0=pt[0]
    p1=pt[1]
    while i<len(pt)-1:
        p0=pt[i-2]
        p1=pt[i-1]
        p2=pt[i]
        p3=pt[i+1]
        print(p0[0],p1[0],p2[0],p3[0])

        if p2[1]>=p0[1] and p3[1]>p1[1] and p2[1]<p1[1] :     #上升通道
            print('A')
            zf1 = abs(p2[1]/p1[1]-1)*100     #上升幅度
            zf2 = abs(p3[1]/p2[1]-1)*100     #上升幅度
            if (zf1<tv1 or zf2<tv2):
                pt.remove(p1)
                pt.remove(p2)
                i -= 2
            else:
                i += 1

        elif p2[1]<=p0[1] and p3[1]<p1[1] and p2[1]>p1[1]:     #下降通道
            print('B')
            zf1 = abs(p2[1]/p1[1]-1)*100     #上升幅度
            zf2 = abs(p3[1]/p2[1]-1)*100     #上升幅度
            if (zf1<tv1 or zf2<tv2):
                pt.remove(p1)
                pt.remove(p2)
                i -= 2
            else:
                i += 1
                
#        if p3[1]>p1[1] and p2[1]<p1[1]:   
#            zf = abs(p2[1]/p1[1]-1)*100     #V顶处理
#            if zf<tv:
#                pt.remove(p1)
#                pt.remove(p2)
#            else:
#                i += 1
        
        else:
            i += 1

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
        
        
    return pout,tout


def best_peaks_troughs1(peaks,troughs,tv):

    #计算波峰、波谷满足涨跌幅的情况
#    tv=30
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

#    p=list(set([e[1] for e in t]))
#    
#    t=list(set([e[1] for e in p]))
#    
#    p.sort(key=lambda x:x[0])           
#    t.sort(key=lambda x:x[0])       

    return p,t

def find_path(p,t):

    pt=p+t
    pt.sort(key=lambda x:x[0])
    
    point1=list(set([e[0] for e in pt]))
    point2=list(set([e[1] for e in pt]))
    point=list(set(point1+point2))
    point.sort(key=lambda x:x[0])
    
    g={}
    
    for e in point:
        k   = 'v%d' % e[0]
        v = []
        for em in pt:
            if em[0][0]==e[0]:
                v.append('v%d' % em[1][0])
        g[k] = v
        
        
        


if __name__ == "__main__":

    df = pd.read_csv(r'd:\selestock\sz300349.csv', dtype={'date':'object'})
    h=df['price'].tolist()
    fig = plt.figure(figsize=(60,20))
    plt.plot(np.arange(len(h)),h)

#    peaks,troughs = get_peaks_troughs(h,10)
    peaks,troughs = get_peaks_troughs1(h)

    peaks,troughs = best_peaks_troughs(peaks,troughs,20)

    for x,y in peaks:
        plt.text(x,y,y,fontsize=10,verticalalignment="bottom",horizontalalignment="center")
    for x,y in troughs:
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
 