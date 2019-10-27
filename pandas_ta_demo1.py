# -*- coding: utf-8 -*-
"""
Created on Mon Sep 23 14:12:27 2019

@author: lenovo
"""

import pandas as pd
import pandas_ta as ta
import pandas_hybta as hybta

df = pd.read_csv(r'F:\pandas-ta\data\sample.csv',index_col='date', parse_dates=True)
df.index=pd.to_datetime(df.index)
df=df.drop(columns='Unnamed: 0')

df.hybta.ao(append=True)

df.ta.adjusted

df.ta.percent_return(cumulative=True, append=True)

df.ta.ichimoku(append=True)
#
df4=df.ta.ao()

apo = df.ta(kind='apo', timed=True)

df.ta.constants(False, -4, 4, 1)

inds=df.ta.indicators(exclude=['ao'],as_list=True)

levels = [x for x in range(-100, 101) if x % 2 == 0]

ta_indicators = list((x for x in dir(pd.DataFrame().ta) if not x.startswith('_') and not x.endswith('_')))

kwargs={'exclude':['ad','ao'],}
exclude_methods = kwargs.pop('exclude', None)

