# -*- conding: utf-8 -*-

import pymysql,pymssql

# pymssql.Binary = bytearray

class MYSQLManager(object):
    # 初始化实例方法
    def __init__(self,result_type=''):
        self.result_type = result_type
        self.conn = None
        self.cursor = None
        self.connect()

    # 连接数据库
    def connect(self):
        self.conn = pymysql.connect(
            host='',
            port=,
            database='test',
            user='user',
            password='passwd',
            charset='utf8'
        )
        if self.result_type == '':
            self.cursor = self.conn.cursor(cursor=pymysql.cursors.DictCursor)
        elif self.result_type == 'str':
            self.cursor = self.conn.cursor()

    # 查询多条数据sql是sql语句，args是sql语句的参数
    def get_list(self, sql, args=None):
        self.cursor.execute(sql, args)
        result = self.cursor.fetchall()
        return result

    # 查询单条数据
    def get_one(self, sql, args=None):
        self.cursor.execute(sql, args)
        result = self.cursor.fetchone()
        return result

    # 执行单条SQL语句
    def moddify(self, sql, args=None):
        self.cursor.execute(sql, args)
        self.conn.commit()

    # 执行多条SQL语句
    def multi_modify(self, sql, args=None):
        self.cursor.executemany(sql, args)
        self.conn.commit()

    # 创建单条记录的语句
    def create(self, sql, args=None):
        self.cursor.execute(sql, args)
        self.conn.commit()
        last_id = self.cursor.lastrowid
        return last_id

    # 关闭数据库cursor和连接
    def close(self):
        self.cursor.close()
        self.conn.close()

    # 进入with语句自动执行
    def __enter__(self):
        return self

    # 退出with语句块自动执行
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class MSSQLManager(object):
    # 初始化实例方法
    def __init__(self, is_autocommit='false'):
        self.is_autocommit= is_autocommit
        self.conn = None
        self.cursor = None
        self.connect()

    # 连接数据库
    def connect(self):
        if self.is_autocommit == "false":
            self.conn = pymssql.connect(
                host='',
                port=1433,
                # database='',
                user='sa',
                password='',
                charset='utf8'
            )
            self.cursor = self.conn.cursor(as_dict=True)
        elif self.is_autocommit == "true":
            self.conn = pymssql.connect(
                host='192.168.20.76',
                port=1433,
                # database='edoc2v5',
                user='sa',
                password='1qaz2WSX',
                charset='utf8',
                autocommit=True
            )
            self.cursor = self.conn.cursor(as_dict=True)

    # 查询多条数据sql是sql语句，args是sql语句的参数
    def get_list(self, sql, args=None):
        self.cursor.execute(sql, args)
        result = self.cursor.fetchall()
        return result

    # 查询单条数据
    def get_one(self, sql, args=None):
        self.cursor.execute(sql, args)
        result = self.cursor.fetchone()
        return result

    # 执行单条SQL语句
    def moddify(self, sql, args=None):
        self.cursor.execute(sql, args)
        self.conn.commit()

    # 执行多条SQL语句
    def multi_modify(self, sql, args=None):
        self.cursor.executemany(sql, args)
        self.conn.commit()

    # 创建单条记录的语句
    def create(self, sql, args=None):
        self.cursor.execute(sql, args)
        self.conn.commit()
        last_id = self.cursor.lastrowid
        return last_id

    # 关闭数据库cursor和连接
    def close(self):
        self.cursor.close()
        self.conn.close()

    # 进入with语句自动执行
    def __enter__(self):
        return self

    # 退出with语句块自动执行
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
