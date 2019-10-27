# -*- coding: utf-8 -*-
"""
Created on Wed May  8 14:14:48 2019

@author: lenovo
python global 用法
https://blog.csdn.net/budong282712018/article/details/80236191

"""

def func():
    global x

    print('x is', x)
    x = 2
    print('Changed local x to', x)

x = 50
func()
print('Value of x is', x)