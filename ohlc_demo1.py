# -*- coding: utf-8 -*-
"""
Created on Wed May  1 22:09:08 2019

@author: lenovo
"""

import pandas as pd
import tushare as ts
api=ts.get_apis()
stock_day=ts.bar('600998',conn=api,start_date='2010-01-01')

# 1、必须将时间索引类型编程Pandas默认的类型
stock_day=stock_day.sort_index()
stock_day.index=pd.to_datetime(stock_day.index)

# 2、进行频率转换日K---周K,首先让所有指标都为最后一天的价格
period_week_data = stock_day.resample('W').last()

# 分别对于开盘、收盘、最高价、最低价进行处理
period_week_data['open'] = stock_day['open'].resample('W').first()
# 处理最高价和最低价
period_week_data['high'] = stock_day['high'].resample('W').max()
# 最低价
period_week_data['low'] = stock_day['low'].resample('W').min()
# 成交量、成交额 这一周的每天成交量的和
period_week_data['vol'] = stock_day['vol'].resample('W').sum()
period_week_data['amount'] = stock_day['amount'].resample('W').sum()

#对于其中存在的缺失值
period_week_data=period_week_data.dropna(axis=0,thresh=8)

#下面两条语句均可
period_week_data.loc[:,'p_change']=period_week_data.close.pct_change()*100

period_week_data['p_change']=period_week_data.close.pct_change()*100

s=period_week_data['close']
p=(s-s.shift(1))/s.shift(1)*100
period_week_data['p_change']=p
