# -*- coding: utf-8 -*-
"""
Created on Sun Feb  3 16:48:06 2019

@author: lenovo
"""

import sched
import time

sche = sched.scheduler(time.time, time.sleep)

def print_time(a='default'):
    print('From print_time', time.time(), a)

def print_some_time():
    print(time.time())
    # 10是delay单位是毫秒, 1代表优先级
    sche.enter(10, 1, print_time)
    sche.enter(5, 2, print_time, argument=('positional',))
    sche.enter(5, 1, print_time, kwargs={'a': 'keyword'})
    sche.run()
    print(time.time())

print_some_time()