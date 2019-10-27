# -*- coding: utf-8 -*-
"""
Created on Wed Aug 28 16:58:59 2019

Python折腾数据库（一）peewee
https://www.jianshu.com/p/84e667320ab3

#Pyhton# 使用Peewee玩转SQL数据库--导入数据
https://www.jianshu.com/p/26648d192cd4


Python轻量级ORM框架Peewee访问sqlite数据库的方法详解
https://www.jb51.net/article/119141.htm

PEEWEE基本使用
https://www.cnblogs.com/yxi-liu/p/8514763.html


轻量级数据库sqlite与peewee的增删改查操作
https://blog.csdn.net/a_lazy_zhu/article/details/80156893

peewee 使用经验
https://www.cnblogs.com/xueweihan/p/6698456.html


"""

from peewee import *
db = SqliteDatabase('test.db')

class BaseModel(Model):
  class Meta:
    database = db

class Course(BaseModel):
  id = PrimaryKeyField()
  title = CharField(null = False)
  period = IntegerField()
  description = CharField()
  class Meta:
    order_by = ('title',)
    db_table = 'course'

class Teacher(BaseModel):
  id = PrimaryKeyField()
  name = CharField(null = False)
  gender = BooleanField()
  address = CharField()
  course_id = ForeignKeyField(Course,to_field='id',related_name = "course")
  class Meta:
    order_by = ('name',)
    db_table = 'teacher'
    
    
Course.create_table()
Teacher.create_table()
Course.create(id = 1,title='经济学',period = 320,description='文科必修')
Course.create(id = 2,title='大学语文',period = 300,description='所有学科必修')
Course.create(id = 3,title='操作系统',period = 320,description='计算机必修')
Course.create(id = 4,title='马克思主义哲学',period = 320,description='必修')
Teacher.create(id = 1,name = '张三',gender=True,address='...',course_id = 1)
Teacher.create(id = 2,name = '李四',gender=False,address='-',course_id = 2)
Teacher.create(id = 3,name = '王五',gender=True,address='=',course_id = 3)
    