# -*- coding: utf-8 -*-
"""
Created on Tue Aug 21 13:25:29 2018

@author: lenovo
"""

from urllib.request import urlopen, Request
import re
import json
import pandas as pd

def get_data(types,pgn):

    url='http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?num=80&sort=code&asc=0&node=%s&symbol=&_s_r_a=page&page=%s' % (types,pgn)
    request = Request(url)


    text = urlopen(request, timeout=10).read()
    text = text.decode('gbk')

    #将JavaScript 数据转换成JSON
    #在"键名"两边加双引号
    #{symbol:"sh603838",code:"603838",name:"四通股份",trade:"10.030"
    #{"symbol":"sh603838","code":"603838","name":"四通股份","trade":"10.030" 
    
    reg = re.compile(r'\,(.*?)\:') 
    text = reg.sub(r',"\1":', text) 
    
    #替换除第1个的
    text = text.replace('"{symbol', '{"symbol')
    #替换第1个
    text = text.replace('{symbol', '{"symbol"')
    
    jstr = json.dumps(text)
    
    js = json.loads(jstr)
    
    df = pd.DataFrame(pd.read_json(js, dtype={'code':object}),
                  columns=DAY_TRADING_COLUMNS)
    
    #df = df.drop('symbol', axis=1)
    return df

if __name__ == "__main__":  
    DAY_TRADING_COLUMNS = ['code', 'symbol', 'name', 'changepercent',
                           'trade', 'open', 'high', 'low', 'settlement', 'volume', 'turnoverratio',
                           'amount', 'per', 'pb', 'mktcap', 'nmc']

    df = get_data('hs_a', 1)
    if df is not None:
        for i in range(2, 60):
            newdf = get_data('hs_a', i)
            df = df.append(newdf, ignore_index=True)
    df = df.append(get_data('shfxjs', 1), ignore_index=True)
    df = df.set_index('symbol') 
    
    


