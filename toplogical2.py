# -*- coding: utf-8 -*-
"""
Created on Sat Mar  9 17:19:37 2019

https://blog.csdn.net/lanchunhui/article/details/50957608

"""


def topsort(G): 
    in_degrees = dict((u, 0) for u in G) 
    for u in G: 
        for v in G[u]: 
            in_degrees[v] += 1 # 每一个节点的入度 
            
    Q = [u for u in G if in_degrees[u] == 0] # 入度为 0 的节点 
    S = [] 
    while Q: 
        u = Q.pop() 
        
        # 默认从最后一个移除 
        
        S.append(u) 
        for v in G[u]: 
            in_degrees[v] -= 1 
            
            # 并移除其指向 
            
            if in_degrees[v] == 0: 
                Q.append(v) 
                        
    return S

def topsort1(G): 
    in_degrees = dict((u, 0) for u in G) 
    for u in G: 
        for v in G[u]: 
            in_degrees[v] += 1 # 每一个节点的入度 
            
    Q = [u for u in G if in_degrees[u] == 0] # 入度为 0 的节点 
    S = [] 
    while Q: 
        u = Q.pop() 
        
        # 默认从最后一个移除 
        
        S.append(u) 
        for v in G[u]: 
            in_degrees[v] -= 1 
            
            # 并移除其指向 
            
            if in_degrees[v] == 0: 
                Q.append(v) 
                        
    return S

#G = {
#    'a':'bf',
#    'b':'cdf',
#    'c':'d',
#    'd':'ef',
#    'e':'f',
#    'f':''
#}
#
#GG = {
#    'a':['b','f'],
#    'b':['c','d','f'],
#    'c':['d'],
#    'd':['e','f'],
#    'e':['f'],
#    'f':[]
#}

G={'START':['v0'],
     'v0': ['v24', 'v39', 'v65', 'v69', 'v106', 'v120'],
     'v6': ['v24', 'v39', 'v65', 'v69', 'v106', 'v120'],
     'v8': ['v24', 'v39', 'v65', 'v69', 'v106', 'v120'],
     'v11': ['v24', 'v39', 'v65', 'v69', 'v106', 'v120'],
     'v15': ['v24', 'v39', 'v65', 'v69', 'v106', 'v120'],
     'v18': ['v24', 'v39', 'v65', 'v69', 'v106', 'v120'],
     'v24': ['v27'],
     'v27': ['v39', 'v65', 'v69', 'v106', 'v120'],
     'v33': ['v39', 'v65', 'v69', 'v106', 'v120'],
     'v36': ['v65', 'v69', 'v106', 'v120'],
     'v39': ['v44'],
     'v40': ['v65', 'v69', 'v106', 'v120'],
     'v42': ['v65', 'v69', 'v106', 'v120'],
     'v44': ['v65', 'v69', 'v106', 'v120'],
     'v47': ['v65', 'v69', 'v106', 'v120'],
     'v49': ['v65', 'v69', 'v106', 'v120'],
     'v52': ['v106', 'v120'],
     'v55': ['v106', 'v120'],
     'v61': ['v65', 'v69', 'v106', 'v120'],
     'v65': ['v73'],
     'v67': ['v106', 'v120'],
     'v69': ['v73'],
     'v70': ['v106', 'v120'],
     'v73': ['v106', 'v120'],
     'v82': ['v106', 'v120'],
     'v87': ['v106', 'v120'],
     'v91': ['v106', 'v120'],
     'v93': ['v106', 'v120'],
     'v98': ['v106', 'v120'],
     'v100': ['v106', 'v120'],
     'v106': ['v123'],
     'v120': ['v123'],
     'v123': []}
print(topsort(G))