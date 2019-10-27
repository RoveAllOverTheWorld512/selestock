# -*- coding: utf-8 -*-
"""
Created on Thu Jul  5 20:08:52 2018

@author: lenovo
"""

import socket

s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect(('8.8.8.8', 80))
ip = s.getsockname()[0]
s.close()
print(ip)
