# -*- coding: utf-8 -*-
"""
Created on Wed Aug 28 12:52:00 2019

@author: lenovo

https://www.yinyubo.cn/?p=376&mip

https://www.cnblogs.com/Jerryshome/archive/2013/01/30/2882931.html

"""

import sqlite3
from Queue import Queue
from threading import Thread

class SqliteMultithread(Thread):
    """
    Wrap sqlite connection in a way that allows concurrent requests from multiple threads.

    This is done by internally queueing the requests and processing them sequentially
    in a separate thread (in the same order they arrived).

    """
    def __init__(self, filename, autocommit, journal_mode):
        super(SqliteMultithread, self).__init__()
        self.filename = filename
        self.autocommit = autocommit
        self.journal_mode = journal_mode
        self.reqs = Queue() # use request queue of unlimited size
        self.setDaemon(True) # python2.5-compatible
        self.start()

    def run(self):
        #自动提交模式:isolation_level=None
        #http://doc.okbase.net/tinyhare/archive/251147.html
        #check_same_thread这个设置为False，即可允许sqlite被多个线程同时访问
        #https://blog.csdn.net/blueheart20/article/details/70218102
        if self.autocommit:
            conn = sqlite3.connect(self.filename, isolation_level=None, check_same_thread=False)
        else:
            conn = sqlite3.connect(self.filename, check_same_thread=False)
            
        conn.execute('PRAGMA journal_mode = %s' % self.journal_mode)
        conn.text_factory = str
        cursor = conn.cursor()
        
        '''
        https://blog.csdn.net/qinlicang/article/details/6079453
        若将 synchronous 设为 OFF (0)，那么 SQLite 从操作系统取得数据后将会立即进行
        处理而不会暂停。 如果使用 SQLite 的应用程序崩溃，数据将是安全的。 但如果在数据
        写入磁盘之前，操作系统死机或机器断电， 数据库文件可能会损坏。此外，在该模式下，
        某些操作会比其它情况下快 50 倍。
        '''
        cursor.execute('PRAGMA synchronous=OFF')
        while True:
            req, arg, res = self.reqs.get()
            if req == '--close--':
                break
            elif req == '--commit--':
                conn.commit()
            else:
                cursor.execute(req, arg)
                if res:
                    for rec in cursor:
                        res.put(rec)
                    res.put('--no more--')
                if self.autocommit:
                    conn.commit()
        conn.close()

    def execute(self, req, arg=None, res=None):
        """
        `execute` calls are non-blocking: just queue up the request and return immediately.

        """
        self.reqs.put((req, arg or tuple(), res))

    def executemany(self, req, items):
        for item in items:
            self.execute(req, item)

    def select(self, req, arg=None):
        """
        Unlike sqlite's native select, this select doesn't handle iteration efficiently.

        The result of `select` starts filling up with values as soon as the
        request is dequeued, and although you can iterate over the result normally
        (`for res in self.select(): ...`), the entire result will be in memory.

        """
        res = Queue() # results of the select will appear as items in this queue
        self.execute(req, arg, res)
        while True:
            rec = res.get()
            if rec == '--no more--':
                break
            yield rec

    def select_one(self, req, arg=None):
        """Return only the first row of the SELECT, or None if there are no matching rows."""
        try:
            return iter(self.select(req, arg)).next()
        except StopIteration:
            return None

    def commit(self):
        self.execute('--commit--')

    def close(self):
        self.execute('--close--')

#endclass SqliteMultithread