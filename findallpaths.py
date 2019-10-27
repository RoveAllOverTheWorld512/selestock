# -*- coding: utf-8 -*-
"""
Created on Sun Mar 10 16:27:49 2019

https://www.geeksforgeeks.org/find-paths-given-source-destination/


https://blog.csdn.net/real_ray/article/details/17919289

"""

# Python program to print all paths from a source to destination. 

from collections import defaultdict 

#This class represents a directed graph 
# using adjacency list representation 
#此类使用邻接列表表示法表示有向图
class Graph: 

	def __init__(self,vertices): 
		#No. of vertices 顶点个数
		self.V= vertices 
		
		# default dictionary to store graph 存储图形的默认字典
		self.graph = defaultdict(list) 

	# function to add an edge to graph 
	def addEdge(self,u,v): 
		self.graph[u].append(v) 

	'''A recursive function to print all paths from 'u' to 'd'. 
	visited[] keeps track of vertices in current path. 
	path[] stores actual vertices and path_index is current 
	index in path[]
    用于打印从“u”到“d”的所有路径的递归函数。visited[]跟踪当前路径中的顶点。
    path[]存储实际顶点，路径索引为当前路径索引path[]
    '''
	def printAllPathsUtil(self, u, d, visited, path): 

		# Mark the current node as visited and store in path
        #将当前节点标记为已访问并存储在路径中 
		visited[u]= True
		path.append(u) 

		# If current vertex is same as destination, then print 
		# current path[] 
        #如果当前顶点与目标相同，则打印
        #当前路径[]
		if u ==d: 
			print(path) 
		else: 
			# If current vertex is not destination 
			#Recur for all the vertices adjacent to this vertex
            #如果当前顶点不是目标
            #对与此顶点相邻的所有顶点递归
			for i in self.graph[u]: 
				if visited[i]==False: 
					self.printAllPathsUtil(i, d, visited, path) 
					
		# Remove current vertex from path[] and mark it as unvisited 
		#从path[]中删除当前顶点并将其标记为未访问
        
		path.pop() 
		visited[u]= False


	# Prints all paths from 's' to 'd' 
	def printAllPaths(self,s, d): 

		# Mark all the vertices as not visited 
		visited =[False]*(self.V) 

		# Create an array to store paths 
		path = [] 

		# Call the recursive helper function to print all paths 
		self.printAllPathsUtil(s, d,visited, path) 



# Create a graph given in the above diagram 
g = Graph(4) 
g.addEdge(0, 1) 
g.addEdge(0, 2) 
g.addEdge(0, 3) 
g.addEdge(2, 0) 
g.addEdge(2, 1) 
g.addEdge(1, 3) 

s = 2 ; d = 3
print("Following are all different paths from %d to %d :" %(s, d)) 
g.printAllPaths(s, d) 
#This code is contributed by Neelam Yadav 
