# -*- coding: utf-8 -*-
"""
Created on Wed Aug  1 18:38:20 2018

@author: lenovo
"""

import subprocess  
   
def main():  
    process1 = subprocess.Popen("python  -u sub.py", shell=False, stdout = subprocess.PIPE, stderr=subprocess.STDOUT) 


    #print process1.communicate()[0]  
   
    while True:  
        line = process1.stdout.readline()  
        if not line:  
            break  
        print(line)  
       
if __name__ == '__main__':  
    main() 