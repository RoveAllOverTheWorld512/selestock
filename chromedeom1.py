# -*- coding: utf-8 -*-
"""
Created on Thu Jul  5 11:00:49 2018

@author: lenovo

Selenium+Python：下载文件(Firefox 和 Chrome)
https://blog.csdn.net/Momorrine/article/details/79794146

headless不能与


https://intoli.com/blog/running-selenium-with-headless-chrome/

selenium无界面chromedriver
http://www.cnblogs.com/z-x-y/p/9026226.html


"""

import os
from selenium import webdriver 
from selenium.webdriver.chrome.options import Options 
import time 
chrome_options = Options() 
#chrome_options.set_headless()
chrome_options.add_argument("headless") 
#chrome_options.add_argument("--headless") 
chrome_options.add_argument('--disable-gpu')

chrome_options.add_argument('blink-settings=imagesEnabled=false')

prefs = {'profile.default_content_settings.popups': 0, 'download.default_directory': os.getcwd()}
#prefs = {'profile.default_content_settings.popups': 0, 'download.default_directory': r'D:\selestock'}
chrome_options.add_experimental_option('prefs', prefs)
driver = webdriver.Chrome(chrome_options=chrome_options) 

#base_url = "http://www.baidu.com/" 
#
##driver.get(base_url) 
#
#driver.get('http://sahitest.com/demo/saveAs.htm')
#start_time=time.time() 
#print('this is start_time ',start_time) 
#driver.find_element_by_id("kw").send_keys("selenium webdriver") 
#driver.find_element_by_id("su").click() 
#driver.save_screenshot('screen.png') 
#driver.close() 
#end_time=time.time() 
#print('this is end_time ',end_time)

driver.get('http://sahitest.com/demo/saveAs.htm')
driver.find_element_by_xpath('//a[text()="testsaveas.zip"]').click()
time.sleep(3)
driver.quit()