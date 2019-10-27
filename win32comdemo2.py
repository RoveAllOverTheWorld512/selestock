# -*- coding: utf-8 -*-
"""
Created on Tue Aug  7 17:39:48 2018

@author: lenovo

Python通过win32 处理Word
https://blog.csdn.net/jazywoo123/article/details/18356713


Python操作word常见方法示例【win32com与docx模块】
https://www.jb51.net/article/143936.htm


"""

#把数据库中的表结构导出到word的表格中，完成设计文档
#不会用win32com操作word样式
import sqlite3
from win32com.client import Dispatch

dbfn='d:\\hyb\\STOCKDSTX.db'

dbcn = sqlite3.connect(dbfn)
curs = dbcn.cursor()

sql = "select name from sqlite_master where type = 'table'"
curs.execute(sql)
tables = curs.fetchall()
tables = [e[0] for e in tables]
word = Dispatch('Word.Application')
word.Visible = 1  
word.DisplayAlerts = 0 
doc = word.Documents.Add()
r = doc.Range(0,0)
r.Style.Font.Name = "Verdana"
r.Style.Font.Size = "9"

for table_name in tables:

    sql = "PRAGMA table_info(%s)" % table_name 
    curs.execute(sql)
    result = curs.fetchall()
    print(r.Start)
    r.InsertBefore("\n" + table_name)
    table = r.Tables.Add(doc.Range(r.End,r.End),len(result) + 1,6)
    table.Rows[0].Cells[0].Range.Text = "列号"
    table.Rows[0].Cells[1].Range.Text = "列名"
    table.Rows[0].Cells[2].Range.Text = "类型"
    table.Rows[0].Cells[3].Range.Text = "是否为空"
    table.Rows[0].Cells[4].Range.Text = "默认值"
    table.Rows[0].Cells[5].Range.Text = "是否主键"
    for i,column in enumerate(result):
        for j,col in enumerate(column):
            if col == None:
                col = "(NULL)"
            table.Rows[i+1].Cells[j].Range.Text = col


    r = doc.Range(table.Range.End,table.Range.End)

