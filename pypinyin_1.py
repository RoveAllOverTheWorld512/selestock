# -*- coding: utf-8 -*-
"""
Created on Thu Jun 13 20:32:31 2019

@author: lenovo
"""
import pypinyin

# 不带声调的(style=pypinyin.NORMAL) 

def pinyin(word): 
    s = '' 
    for i in pypinyin.pinyin(word, style=pypinyin.NORMAL): 
        s += ''.join(i) 
        
    return s 

# 带声调的(默认) 

def yinjie(word): 
    s = '' 
    # heteronym=True开启多音字 
    for i in pypinyin.pinyin(word, heteronym=True): 
        s = s + ''.join(i) + " " 
        
    return s 

def pinyin_first(word):
    return pypinyin.slug(word, heteronym=True, style=pypinyin.FIRST_LETTER).replace('-','').upper()

if __name__ == "__main__": 
    print(pinyin("忠厚传家久")) 
    print(yinjie("诗书继世长"))
    print(pinyin_first("诗书继世长"))

