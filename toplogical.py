# -*- coding: utf-8 -*-
"""
Created on Sat Mar  9 16:46:41 2019

@author: lenovo
"""

def topological_sort( graph ):
 
    is_visit = dict( ( node, False ) for node in graph )
    li = []
 
    def dfs( graph, start_node ):
        
        for end_node in graph[start_node]:
            if not is_visit[end_node]:
                is_visit[end_node] = True
                dfs( graph, end_node )
        li.append( start_node )
    
    for start_node in graph:
        if not is_visit[start_node]:
            is_visit[start_node] = True
            dfs( graph, start_node )
 
    li.reverse()
    return li
    
            
if __name__ == '__main__':
    graph = {
        'v1': ['v5'],
        'v2': ['v1'],
        'v3': ['v1', 'v5'],
        'v4': ['v2', 'v5'],
        'v5': [],
        'v6': [],
    }
    li = topological_sort( graph )
    print(li)

    g = {'v0': ['v24', 'v39', 'v65', 'v69', 'v106', 'v120'],
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
         'v123':[],
         }
    
    l = topological_sort( graph )
    print(l)
