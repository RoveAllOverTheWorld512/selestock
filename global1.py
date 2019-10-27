# -*- coding: utf-8 -*-
"""
Created on Thu Aug  9 22:38:29 2018

@author: lenovo
"""

def sub0():
    print('sub0_a=%d' % a)
    
def sub1():
    global a
    a=2
    print('sub1_a=%d' % a)

def sub2(a):
    a=3
    print('sub2_a=%d' % a)
    
if __name__ == '__main__':
    a=1
    sub0()
    sub1()
    print(a)
    sub2(a)
    print(a)