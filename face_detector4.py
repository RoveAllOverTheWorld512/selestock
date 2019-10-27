# -*- coding: utf-8 -*-
"""
Created on Fri Aug  9 18:10:57 2019

@author: lenovo



原文链接：https://blog.csdn.net/github_39611196/article/details/81240352

"""


import cv2
#import numpy as np
 
# 运行之前，检查cascade文件路径是否在相应的目录下
#face_cascade = cv2.CascadeClassifier(r'D:\Anaconda3\Lib\site-packages\cv2\data\haarcascade_frontalface_alt_tree.xml')
#face_cascade = cv2.CascadeClassifier(r'D:\Anaconda3\Lib\site-packages\cv2\data\haarcascade_frontalcatface_extended.xml')
#face_cascade = cv2.CascadeClassifier(r'D:\Anaconda3\Lib\site-packages\cv2\data\haarcascade_frontalcatface.xml')
#face_cascade = cv2.CascadeClassifier(r'D:\Anaconda3\Lib\site-packages\cv2\data\haarcascade_frontalface_alt.xml')
#face_cascade = cv2.CascadeClassifier(r'D:\Anaconda3\Lib\site-packages\cv2\data\haarcascade_frontalface_alt2.xml')
face_cascade = cv2.CascadeClassifier(r'D:\Anaconda3\Lib\site-packages\cv2\data\haarcascade_frontalface_default.xml')
eye_cascade = cv2.CascadeClassifier(r'D:\Anaconda3\Lib\site-packages\cv2\data\haarcascade_eye.xml')
#eye_cascade = cv2.CascadeClassifier(r'D:\Anaconda3\Lib\site-packages\cv2\data\haarcascade_eye_tree_eyeglasses.xml')

#face_cascade = cv2.CascadeClassifier(r'f:\photo\haarcascade_frontalface_default2.xml')
#eye_cascade = cv2.CascadeClassifier(r'f:\photo\haarcascade_eye2.xml')
 
# 读取图像
img =cv2.imread(r'f:\photo\3.jpg')
gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
 
# 检测脸部
faces = face_cascade.detectMultiScale(
        gray, 
        scaleFactor=1.1, 
        minNeighbors=5, 
        minSize=(25, 25), 
        flags=cv2.CASCADE_SCALE_IMAGE
        )

print('Detected ', len(faces), " face")
 
for (x, y, w, h) in faces:
    img = cv2.rectangle(img, (x, y), (x + w, y + h), (255, 0, 0), 1)
    roi_gray = gray[y: y + h, x: x + w]
    roi_color = img[y: y + h, x: x + w]
    
    eyes = eye_cascade.detectMultiScale(roi_gray)
    for(ex, ey, ew, eh) in eyes:
        cv2.rectangle(roi_color, (ex, ey), (ex + ew, ey + eh), (0, 255, 0), 1)
 
cv2.imshow('img', img)
cv2.waitKey(0)
cv2.destroyAllWindows()
