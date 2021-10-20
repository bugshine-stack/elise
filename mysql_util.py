# -*- coding: UTF-8 -*-
"""
@author: kongxiangshuai
@file: mysql_util.py
@time: 2021/10/07
"""
import pymysql


host = 'localhost'
port = 3306
db = 'elise'
user = 'root'
password = '123456'


# ---- 用pymysql 操作数据库
def get_connection():
    conn = pymysql.connect(host=host, port=port, db=db, user=user, password=password)
    return conn


def check_it():

    conn = get_connection()

    # 使用 cursor() 方法创建一个 dict 格式的游标对象 cursor
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    # 使用 execute()  方法执行 SQL 查询
    cursor.execute("select count(id) as total from Product")

    # 使用 fetchone() 方法获取单条数据.
    data = cursor.fetchone()

    print("-- 当前数量: %d " % data['total'])

    # 关闭数据库连接
    cursor.close()
    conn.close()


if __name__ == '__main__':
    check_it()
