# -*- coding: utf-8 -*-
"""
Created on Sat Aug 24 11:02:38 2019

@author: lenovo
"""

import time
import urllib
from http.cookiejar import CookieJar
 
#声明一个CookieJar对象实例来保存cookie
cookie = CookieJar()
#利用urllib2库的HTTPCookieProcessor对象来创建cookie处理器
handler=urllib.request.HTTPCookieProcessor(cookie)
#通过handler来构建opener
opener = urllib.request.build_opener(handler)
#此处的open方法同urllib2的urlopen方法，也可以传入request
urllib.request.install_opener(opener)

for _ in range(2):
    response = opener.open('https://asos.tmall.com/search.htm').read()
                           #?spm=a1z10.5-b.w4011-5044691060.102.QqiXRs')
    for item in cookie:
        print('Name = %s' % item.name)
        print('Value = %s' % item.value)
    time.sleep(1)
    print('='*40)