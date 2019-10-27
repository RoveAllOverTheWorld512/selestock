# -*- coding: utf-8 -*-
"""
Created on Mon Jan 14 19:27:12 2019

@author: lenovo

fake-useragent，python爬虫伪装请求头
https://blog.csdn.net/qq_29186489/article/details/78496747

python爬取带stonefont字体网页的经历
https://blog.csdn.net/qq_42044484/article/details/80523164


"""

import requests
from fake_useragent import UserAgent
from lxml import etree
import re
from urllib.request import urlretrieve
from fontTools.ttLib import TTFont

def get_response():
    url = 'https://www.shixiseng.com/interns?k=python&p=1'
    headers = {
        'User-Agent': UserAgent().random
    }
    response = requests.get(url, headers=headers)
    return response
 
# 获取实习僧的字体文件
def get_font_url():
    response = get_response()
    font_url = re.findall(r'myFont; src: url\("(.*?)"\)}',response.text,re.S)[0]
    urlretrieve(font_url,'shixiseng_font.woff')
    
# 解析字体文件 ，获取相应的字体映射关系
def parse_font():
    font1 = TTFont('shixiseng_font.woff')
    keys,values = [],[]
    for k, v in font1.getBestCmap().items():
        if v.startswith('uni'):
            keys.append(eval("u'\\u{:x}" .format(k) + "'"))
            values.append(chr(int(v[3:], 16)))
        else:
            keys.append("&#x{:x}".format(k))
            values.append(v)
 
    return keys,values

# 获取数据并对特殊字体转码
def get_data():
    response = get_response()
    data = etree.HTML(response.text)
    ul_data = data.xpath('//ul[@class="position-list"]/li')
    for info in ul_data:
        title = info.xpath('.//div[@class="info1"]/div[@class="name-box clearfix"]/a/text()')[0]
        salary = '  |  '.join(info.xpath('.//div[@class="info2"]/div[@class="more"]/span/text()'))
        print(title,salary)
        print('----------分界线----------')
        keys,values = parse_font()
        for k,v in zip(keys,values):
            title = title.replace(k, v)
            salary = salary.replace(k,v)
        print(title, salary)
        
if __name__ == '__main__':
        
    get_font_url()
    get_data()        