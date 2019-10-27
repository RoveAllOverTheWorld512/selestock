# -*- coding: utf-8 -*-
"""
Created on Sun Mar 10 10:09:33 2019

#原文：https://blog.csdn.net/kan2281123066/article/details/72892431 

https://www.python.org/doc/essays/graphs/

"""


def find_path(graph, start, end, path=[]): 
    path = path + [start] 
    if start == end: 
        return path 
    
    if not start in graph.keys(): 
        return None 
    
    for node in graph[start]: 
        if node not in path: 
            newpath = find_path(graph, node, end, path) 
            
            if newpath: 
                return newpath 
            
    return None


def find_all_paths(graph, start, end, path=[]): 
    path = path + [start] 
    if start == end: 
        return [path] 
    
    if not start in graph.keys(): 
        return [] 
    
    paths = [] 
    
    for node in graph[start]: 
        if node not in path: 
            newpaths = find_all_paths(graph, node, end, path) 
            
            for newpath in newpaths: 
                paths.append(newpath) 
    
    return paths


def find_shortest_path(graph, start, end, path=[]): 
    path = path + [start] 
    if start == end: 
        return path 
    
    if not start in graph.keys(): 
        return None 
    
    shortest = None 
    
    for node in graph[start]: 
        if node not in path: 
            newpath = find_shortest_path(graph, node, end, path) 
            
            if newpath: 
                if not shortest or len(newpath) < len(shortest): 
                    shortest = newpath 
                    
    return shortest


graph = {'V0':['V1','V5'], 
         'V1':['V2'], 
         'V2':['V3'], 
         'V3':['V4','V5'], 
         'V4':['V0'], 
         'V5':['V2','V4']
         }

graph = {'v0': ['v24', 'v39', 'v65', 'v69', 'v106', 'v120'],
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

allpaths=find_all_paths(graph, 'v0','v123')
print(allpaths) 

paths=find_path(graph, 'v0','v123')
print(paths) 

shortestpath=find_shortest_path(graph, 'v0','v123')

print(shortestpath) 
