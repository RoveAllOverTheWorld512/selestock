# -*- coding: utf-8 -*-
"""
Created on Fri Jul 27 10:21:19 2018

@author: lenovo
"""

import sqlite3
import pandas as pd


'''
CREATE TABLE [DFCF1](
      [GPDM] TEXT NOT NULL, 
      [RQ] TEXT NOT NULL,
      [GJ] REAL, 
      [TS1] TEXT, 
      [TS2] TEXT, 
      [TSLX] TEXT NOT NULL);

CREATE UNIQUE INDEX [GPDM_RQ_DFCF1]
    ON [DFCF1](
      [GPDM], 
      [RQ]);

'''

gpdm='002675.SZ'
dbfn='d:\\hyb\\STOCKDSTX.db'
dbcn = sqlite3.connect(dbfn)
#curs = dbcn.cursor()
#
#sql = '''SELECT GPDM,BDRQ,GGMC,BDGS,BDFX,BDJJ,BDSZ,BDLX,ZGBZB,BDHCG,XGGG,GGZW,YGGGX 
#        FROM DFCFGGCGBD WHERE GPDM="%s" ORDER BY BDRQ''' % gpdm
#        
#curs.execute(sql)
#data = curs.fetchall()
#
#cols = 'gpdm,bdrq,ggmc,bdgs,bdfx,bdjj,bdsz,bdlx,zgbzb,bdhcg,xggg,ggzw,ygggx'.split(',')
#
#df = pd.DataFrame(data,columns=cols)
#
#df=df.groupby(['bdrq','bdlx','bdfx'],as_index=False).sum()
#df=df.round(2)
#if len(df)>0: 
#
#    df=df.assign(ts1='高管'+df['bdlx']+'['+df['bdfx']+']'+df['bdsz'].map(lambda x:str(x))+'万元')
#    df=df.assign(gpdm=gpdm)
#    df=df.assign(ts2=None)
#    df=df.assign(tslx='2')
#    
#    df=df[['gpdm','bdrq','ts1','ts2','tslx']]
#
#    data=df.values.tolist()
#
#    dbcn.executemany('''INSERT OR REPLACE INTO DFCF (GPDM,RQ,TS1,TS2,TSLX) 
#                        VALUES (?,?,?,?,?)''', data)
#
#    dbcn.commit()

data=[[gpdm,'2018-07-01','ts1','ts2','0'],
      [gpdm,'2018-07-02','ts1','ts2','1']
      ]
dbcn.executemany('''INSERT OR REPLACE INTO DFCF1 (GPDM,RQ,TS1,TS2,TSLX) 
                    VALUES (?,?,?,?,?)''', data)

data=[[gpdm,'2018-07-01',10,'0'],
      [gpdm,'2018-07-02','10','0']
      ]
dbcn.executemany('''INSERT OR REPLACE INTO DFCF1 (GPDM,RQ,GJ,TSLX) 
                    VALUES (?,?,?,?)''', data)


dbcn.commit()

dbcn.close()
