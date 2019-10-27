# -*- coding: utf-8 -*-
"""
Created on Sat Jul 21 08:38:09 2018

@author: lenovo
"""
from lxml import etree
html = '<table id="table1"cellspacing="0px"><tr><th>编号</th><th>姓名</th><th>年龄</th></tr><tr><td>1</td><td>张三</td><td>11</td></tr><tr><td>2/td><td>李四</td><td>12</td></tr><tr><td>3</td><td>王五</td><td>13</td></tr><tr><td>4</td><td>马六</td><td>14</td></tr></table>'
content = etree.HTML(html)
rows = content.xpath('//table[@id="table1"]/tr')[1:]
for row in rows:
    id = row.xpath('./td[1]/text()')[0]
    name = row.xpath('./td[2]/text()')[0]
    age = row.xpath('./td[3]/text()')[0]
    print(id, name, age)