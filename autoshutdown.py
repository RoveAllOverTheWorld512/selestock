# -*- coding: utf-8 -*-
"""
Created on Wed Aug 15 23:17:30 2018

@author: lenovo
"""

import os
import time
import sys
import re
if len(sys.argv) == 1:
    input_time = input('请输入关机时间，格式如：小时:分钟 ：')
else:
    input_time = sys.argv[1]
    
if input_time == 'off':
    os.system('shutdown -a')

h, m = re.match('(\d{2}):(\d{2})',input_time).groups()
 
h1 = int(h)
m1 = int(m)
 
print(h1, m1)
mytime = time.strftime('%H:%M:%S')
h2 = int(mytime[0:2])
m2 = int(mytime[3:5])
 
if h1 > 24:
    h1 = 24
    m2 = 0
if m1 > 60:
    m1 = 60
if h1 < h2:
    h1 = h1 + 24
 
s1 = (h1+(m1/60.0)-h2-(m2/60.0))*3600
if s1 <= 0:
    print("ERROR")
else:
    print('距离关机还有 %d 秒' %s1)
    os.system('shutdown -s -t %d' %s1)