# -*- coding: utf-8 -*-
"""
Created on Mon Jan 21 17:29:14 2019
识别图片中的文字 - Tesseract 和 百度云OCR的对比
https://segmentfault.com/a/1190000012861561

@author: lenovo
"""

from aip import AipOcr

def get_file_content(filePath):
    with open(filePath, 'rb') as fp:
        return fp.read()

            
if __name__ == '__main__': 

    """ 你的 APPID AK SK """
    APP_ID = '15462445'
    API_KEY = "e41M0Qm2jrt7beaTXR8gwrCy" 
    SECRET_KEY = "kihcmljG1EImRahgubF2FY7DPSBUgA4b" 
    
    client = AipOcr(APP_ID, API_KEY, SECRET_KEY)
    
    filename = r"D:\selestock\test.png" 

    image = get_file_content(filename)
    
    """ 调用通用文字识别, 图片参数为本地图片 """
#    result=client.basicGeneral(image);
    
    """ 如果有可选参数 """
    options = {}
    options["language_type"] = "CHN_ENG"
    options["detect_direction"] = "true"
    options["detect_language"] = "true"
    options["probability"] = "true"
    
    """ 带参数调用通用文字识别, 图片参数为本地图片 """
    result=client.basicGeneral(image, options)
    
    if 'words_result' in result:
        print( '\n'.join([w['words'] for w in result['words_result']]))
