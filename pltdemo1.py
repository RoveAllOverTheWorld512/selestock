# -*- coding: utf-8 -*-
"""
Created on Mon Jul  9 15:41:44 2018

@author: lenovo


Windows下，在Python中用matplotlib画图出现中文乱码（中文变方框）的解决方案
https://blog.csdn.net/mudooo/article/details/80047074

matplotlib-------matplotlibrc
https://blog.csdn.net/xqf1528399071/article/details/53385593

Python matplotlib绘图时汉字显示不正常
https://blog.csdn.net/qingchunlizhi/article/details/59481608

Windows的字体对应名称
http://www.360doc.com/content/14/0713/12/16740871_394080556.shtml

python—matplotlib数据可视化实例注解系列-----设置标注字体样式（matplotlib颜色库）
https://blog.csdn.net/playgoon2/article/details/77965650

matplotlib命令与格式：标题(title),标注(annotate),文字说明(text)
https://blog.csdn.net/helunqu2017/article/details/78659490

"""

import numpy as np
import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif']=['SimHei'] #用来正常显示中文标签
plt.rcParams['axes.unicode_minus']=False #用来正常显示负号

font2 = {'family': 'SimHei',
        'color':  'red',
        'weight': 'normal',
        'size': 16,
        }

plt.plot((1,2,3),(4,5,7))
plt.text(1, 0.35, r'显示汉字和公式：'+r'$\cos(2 \pi x) \exp(-x/3)$', fontdict=font2)
plt.text(1, 2, r'$\cos(2 \pi x) \exp(-x/3)$', fontdict=font2)

plt.xlabel('横坐标')
plt.ylabel('纵坐标')
plt.show()



#---------设置字体样式，分别是字体，颜色，宽度，大小
font1 = {'family': 'Times New Roman',
        'color':  'purple',
        'weight': 'normal',
        'size': 16,
        }
font2 = {'family': 'Times New Roman',
        'color':  'red',
        'weight': 'normal',
        'size': 16,
        }
font3 = {'family': 'serif',
        'color':  'blue',
        'weight': 'bold',
        'size': 14,
        }
font4 = {'family': 'Calibri',
        'color':  'navy',
        'weight': 'normal',
        'size': 17,
        }
#-----------四种不同字体显示风格-----
 
#-------建立函数----------
x = np.linspace(0.0, 5.0, 100)
y = np.cos(2*np.pi*x) * np.exp(-x/3)
#-------绘制图像，添加标注----------
plt.plot(x, y, '--')
plt.title('Damped exponential decay', fontdict=font1)
#------添加文本在指定的坐标处------------
plt.text(2, 0.65, r'$\cos(2 \pi x) \exp(-x/3)$', fontdict=font2)

#---------设置坐标标签
plt.xlabel('Y=time (s)', fontdict=font3)
plt.ylabel('X=voltage(mv)', fontdict=font4)
 
# 调整图像边距
plt.subplots_adjust(left=0.15)
plt.show()




x = np.linspace(-1,2,50)
y1 = 2*x +1
l1, =plt.plot(x,y1,label = 'one')
plt.legend(handles =[l1,] , labels=['1'], loc = 2)

###得到各个边的坐标进行处理

ax = plt.gca()

ax.spines['right'].set_color('none')#右边设置为空
ax.spines['top'].set_color('none')#上边设置为空
ax.xaxis.set_ticks_position('bottom')#下边设置为x轴
ax.yaxis.set_ticks_position('left')#左边设置为y轴
ax.spines['left'].set_position(('data',0))#设置y轴起始点
ax.spines['bottom'].set_position(('data',0))#设置x轴起始点

x0 = 1.5;
y0 = 2*x0 +1
plt.scatter(x0,y0,s = 50,color = 'r',lw= 2)#散列要标注的坐标点
plt.plot([x0,x0],[0,y0],'--')####绘制(x0,0)->(x0,y0)的虚线

##method one
plt.annotate(r'$2x+1=%s$'%y0,
             xy = (x0,y0),
             xycoords = 'data',
             xytext= (+30,-30),
             textcoords = 'offset points',
             fontsize = 16,
             arrowprops = dict(arrowstyle = '->',
             connectionstyle = 'arc3,rad = .2'))
#参数xycoords说明要指向点位置要参考定义的‘data’（0，0）点，xytest表达式相对于xy偏移的量，textcoords表示偏移 ，fontsize 字体大小，arrowprops表示箭头的一些属性 connectionstyle 指的箭头的弯曲弧度

##method two
plt.text(-1,2,r'$There\ are\ some\ expecial\ symbols.\ \mu \ \sigma_i\ \alpha_t$',fontdict={'size':20,'color':'b'})
plt.show()



fig = plt.figure()
plt.axis([0, 10, 0, 10])
t = "This is a really long string that I'd rather have wrapped so that it"\
    " doesn't go outside of the figure, but if it's long enough it will go"\
    " off the top or bottom!"
plt.text(4, 1, t, ha='left', rotation=15, wrap=True)
plt.text(6, 5, t, ha='left', rotation=15, wrap=True)
plt.text(5, 5, t, ha='right', rotation=-15, wrap=True)
plt.text(5, 10, t, fontsize=18, style='oblique', ha='center',va='top',wrap=True)
plt.text(3, 4, t, family='serif', style='italic', ha='right', wrap=True)
plt.text(-1, 0, t, ha='left', rotation=-15, wrap=True)
plt.show()