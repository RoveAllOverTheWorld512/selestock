# -*- coding: utf-8 -*-
"""
Created on Fri Jul 20 20:59:00 2018

@author: lenovo
"""

import lxml.html
url='http://guba.eastmoney.com/list,002376,3,f_1.html'
html = lxml.html.parse(url)     
    
res = html.xpath('//div[@class=\"articleh\"]')

for row in res:
    l3=row.xpath('span[@class=\"l3\"]/a')[0]
    print(l3.attrib['title'])    
    print('%s%s' % ('http://guba.eastmoney.com',l3.attrib['href']))
    
for row in res:
    print(row.xpath('span[@class=\"l3\"]/a/@title')[0]) 
    print('%s%s' % ('http://guba.eastmoney.com',row.xpath('span[@class=\"l3\"]/a/@href')[0]))
    print(row.xpath('span[@class=\"l6\"]/text()'))
