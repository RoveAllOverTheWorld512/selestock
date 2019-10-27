# -*- coding: utf-8 -*-
"""
Created on Fri Aug  9 17:15:11 2019

@author: lenovo
--------------------- 

原文链接：https://blog.csdn.net/qq_38677564/article/details/83830949

其中，人脸训练集haarcascade_frontalface_default.xml和眼睛训练集haarcascade_eye.xml来自github，链接地址如下：https://github.com/opencv/opencv/tree/master/data/haarcascades，请自行下载


"""

import cv2
#import numpy as np

'''
error: OpenCV(4.1.0) C:\projects\opencv-python\opencv\modules\objdetect\src\cascadedetect.cpp:1658: error: (-215:Assertion failed) !empty() in function 'cv::CascadeClassifier::detectMultiScale'
下面的.xml文件要写成绝对路径，否则出错执行到29行

minSize = (5,5),        #目标的最小尺寸

会出错
'''

face_cascade=cv2.CascadeClassifier(r"D:\Anaconda3\Lib\site-packages\cv2\data\haarcascade_frontalface_default.xml")
eye_cascade=cv2.CascadeClassifier(r"D:\Anaconda3\Lib\site-packages\cv2\data\haarcascade_eye.xml")
i = cv2.imread(r'f:\photo\3.jpg')
print (i.shape)
gray=cv2.cvtColor(i,cv2.COLOR_BGR2GRAY)
faces = face_cascade.detectMultiScale(
    gray,                   #要检测的图像
    scaleFactor = 1.1,      #图像尺寸每次缩小的比例
    minNeighbors = 5,       #一个目标至少要检测5次才会被标记为人脸
    minSize = (5,5),        #目标的最小尺寸,这个参数与检测结果有密切关系，太大会漏检
)
l=len(faces)
print (l)
for (x,y,w,h) in faces:
    cv2.rectangle(i,(x,y),(x+w,y+h),(255,0,0),2)
    cv2.putText(i,'face',(w//2+x,y-h//5),cv2.FONT_HERSHEY_PLAIN,2.0,(255,255,255),2,1)
    roi_gray = gray[y:y+h, x:x+w]
    roi_color = i[y:y+h, x:x+w]
    eyes = eye_cascade.detectMultiScale(roi_gray)

cv2.putText(i,"face count",(20,20),cv2.FONT_HERSHEY_PLAIN,2.0,(255,255,255),2,1)
cv2.putText(i,str(l),(230,20),cv2.FONT_HERSHEY_PLAIN,2.0,(255,255,255),2,1)

#cv2.putText(i,"eyes count",(20,60),cv2.FONT_HERSHEY_PLAIN,2.0,(255,255,255),2,1)

print (i.shape)	#cv2.putText(i,str(r),(230,60),cv2.FONT_HERSHEY_PLAIN,2.0,(255,255,255),2,1)

cv2.imshow("img",i)

cv2.waitKey(0)
