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
    url = 'http://data.eastmoney.com/bbsj/201812/yjyg.html'
    headers = {
        'User-Agent': UserAgent().random
    }
    response = requests.get(url, headers=headers)
    return response
 
# 获取实习僧的字体文件
def get_font_url():
    response = get_response()
    
#    font_url = re.findall(r'WoffUrl\":\"(.*?)woff',response.text,re.S)[0]+'woff'
    
    font_url = re.findall(r"url\('(.*?)'\) format\('woff'\);",response.text,re.S)
    
    font_url = re.findall(r',url\(\'(.*?)woff',response.text,re.S)[0]+'woff'

    urlretrieve(font_url,'dfcfw_font.woff')
    
# 解析字体文件 ，获取相应的字体映射关系
def parse_font():
    font_char={'x':'.','qqdwzl':'1','zrwqqdl':'2','wqqdzs':'3',
                 'zbxtdyc':'4','nhpdjl':'5','sxyzdxn':'6',
                 'bgldyy':'7','zwdxtdy':'8','whyhyx':'9','bdzypyc':'0'
                 }
    font1 = TTFont('dfcfw_font.woff')
    
    font1.saveXML('dfcfw_font.ttx')
    keys,values = [],[]
    for k, v in font1.getBestCmap().items():
        keys.append(r"\u{:x}".format(k))
        values.append(font_char[v])

    dic={}
    for k, v in font1.getBestCmap().items():
        if k>128:
            k = bytes(r"\u{:x}".format(k), 'ascii').decode('unicode_escape')
            dic[k]=font_char[v]
 
    return keys,values

# 获取数据并对特殊字体转码
def get_data():
    response = get_response()
#    txt=response.text
#    a=re.findall('\"FontMapping\":\[(.*?)\]',txt)[0]
#    b=re.findall('\"code\":\"(.*?)\"\,\"value\":(.*?)\}',a)
#    for k,v in b:
#        txt=txt.replace(k,v)
    
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
    
    response = get_response()    
#    get_font_url()
#    get_data()        