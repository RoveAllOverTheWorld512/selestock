# -*- coding: utf-8 -*-
"""
Created on Thu Aug  2 17:52:41 2018

@author: lenovo
"""
import datetime
import requests 

import urllib 
from pyquery import PyQuery as pq

url='http://www.tdx.com.cn/list_66_69.html'

html=pq(url)

url = "http://www.tdx.com.cn/products/data/data/vipdoc/shlday.zip"

now1 = datetime.datetime.now().strftime('%H:%M:%S')
print('时间：%s' % now1)

r = requests.get(url) 
with open("tmp2.zip", "wb") as code:
   code.write(r.content)
   


now1 = datetime.datetime.now().strftime('%H:%M:%S')
print('时间：%s' % now1)



urllib.request.urlretrieve(url, "tmp1.zip")
   
now1 = datetime.datetime.now().strftime('%H:%M:%S')
print('时间：%s' % now1)

f = urllib.request.urlopen(url) 
data = f.read() 
with open("demo2.zip", "wb") as code:   
  code.write(data)
  
now1 = datetime.datetime.now().strftime('%H:%M:%S')
print('时间：%s' % now1)
