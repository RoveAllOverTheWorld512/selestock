# -*- coding: utf-8 -*-
"""
Created on Thu Jan 17 16:58:45 2019

@author: lenovo
"""

from fontTools import ttLib, unicode
 
tt = ttLib.TTFont("dfcfw_font.woff")
 
print(tt.getGlyphNames())
print(tt.getGlyphNames2())
print(tt.getGlyphOrder())

glyphs = tt.getGlyphOrder()[2:]
tmp_dic = {}
for num,un_size in enumerate(glyphs):
    print(un_size,num) 
    font_uni = un_size.replace('uni','0x').lower() 
    tmp_dic[font_uni] = num
    
print(tmp_dic) 
