# -*- coding: utf-8 -*-
"""
Created on Wed Aug 28 22:38:13 2019

@author: lenovo

SQLite在多线程环境下的应用
http://mobile.51cto.com/iphone-299765.htm

"""

import sqlite3 
 
import threading 
 
def f(): 
 
    con.rollback() 

 
con = sqlite3.connect('test.db', check_same_thread=False) # 允许在其他线程中使用这个连接 
 
cu = con.cursor() 
 
cu.execute('CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY)') 
 
print(cu.execute('SELECT count(*) FROM test').fetchone()[0]) # 0 
 
cu.execute('INSERT INTO test VALUES (NULL)') 
 
print(cu.execute('SELECT count(*) FROM test').fetchone()[0]) # 1 
 
thread = threading.Thread(target=f) 
 
thread.start() 
 
thread.join() 
 
print(cu.execute('SELECT count(*) FROM test').fetchone()[0]) # 0 
 
cu.close() 
 
con.close() 

