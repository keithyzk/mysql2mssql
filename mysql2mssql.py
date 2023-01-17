# -*- coding: utf-8 -*-

import pyodbc,pymysql,pymssql
import warnings
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL as sqlalchem_URL

# MySQL 连接信息
mysql_host=''
mysql_port=3306
mysql_user='user'
mysql_passwd=''

# SQL Server 连接信息
mssql_host=''
mssql_port='1433'
mssql_user='sa'
mssql_passwd=''

# 要迁移的库名
converDB=""

ms_conn = pymssql.connect(
    host=mssql_host,
    port=mssql_port,
    database=converDB,
    user=mssql_user,
    password=mssql_passwd,
    charset='utf8',
    autocommit=True,
)
ms_cursor = ms_conn.cursor()

warnings.filterwarnings("ignore")

# 字段类型转换
colConverDict = {
    "varchar":"nvarchar",
    "text":"nvarchar(max)",
    "longtext":"nvarchar(max)",
    "mediumtext":"nvarchar(max)",
    "datetime":"datetime2",
    "tinyint":"smallint",
    "longblob":"varbinary(max)",
    "mediumblob":"varbinary(max)",
    "timestamp":"datetime2(3)",
    "double":"float"
}

mysql_conn = pymysql.connect(
            host=mysql_host,
            port=mysql_port,
            database=converDB,
            user=mysql_user,
            password=mysql_passwd,
            charset='utf8',
)

MSEngine = create_engine(sqlalchem_URL.create(
    drivername='mssql+pyodbc',
    username=mssql_user,
    password=mssql_passwd,
    host=mssql_host,
    port=mssql_port,
    database=converDB,
    query={
        "driver": "ODBC Driver 18 for SQL Server",
        "TrustServerCertificate": "yes"
    }
), fast_executemany=True)


def converCol(col):
    col_sp = col.find('(')
    if col_sp != -1:
        # 获取字段类型
        col_type=col[:col_sp]
        # 获取字段长度
        col_length=col[col_sp:]
        # 如果是整数类型，则不需要设置长度
        if "int" in col_type or "timestamp" in col_type:
            col_length=""
    else:
        col_type=col
        col_length=""
    return colConverDict.get(col_type, col_type),col_length

myTabs= pd.read_sql("select TABLE_NAME from information_schema.TABLES where TABLE_SCHEMA='{}' and TABLE_TYPE='BASE TABLE'".format(converDB), mysql_conn)


for t in myTabs['TABLE_NAME']:
    # print(t)
    tbCol = pd.read_sql("SELECT * FROM information_schema.COLUMNS where TABLE_SCHEMA='{0}' and TABLE_NAME='{1}' ORDER BY ORDINAL_POSITION".format(converDB, t), mysql_conn)
    # print(tbCol)

    attr = ""
    for i in range(len(tbCol)):

        # 判断字段类型是否可以为空
        if tbCol["IS_NULLABLE"][i] != "YES":
            null_able = "NOT NULL"
        else:
            null_able = ""

        # 设置字段默认值
        col_default=tbCol["COLUMN_DEFAULT"][i] #.replace("None","NULL").replace("None", "NULL")
        if str(col_default) == 'None':
            set_default="DEFAULT" + " " + str(tbCol["COLUMN_DEFAULT"][i])
        else:
            set_default="DEFAULT" + " '" + str(tbCol["COLUMN_DEFAULT"][i]) + "'"
        set_default=set_default.replace("None","NULL").replace("None","NULL")

        #if tbCol[i]["COLUMN_KEY"] == "PRI":
        #    set_prikey="PRIMARY KEY"
        #else:
        #    set_prikey=""

        if tbCol["EXTRA"][i] == "auto_increment":
            set_prikey="PRIMARY KEY"
        else:
            set_prikey=""


        # 设置字段类型和字段长度
        new_col_type, new_col_length=converCol(tbCol["COLUMN_TYPE"][i])

        # 如果 nvarchar 字段长度大于4000，则设置长度为 max
        if new_col_type == "nvarchar" and new_col_length != "" and int(new_col_length.strip('()')) > 4000:
            new_col_length="(max)"

        #attr += "[" + tbCol[i]["COLUMN_NAME"] + "] " + tbCol[i]["COLUMN_TYPE"] + " " + set_prikey + " " + null_able + set_default + ","
        attr += "[{0}] {1}{2} {3} {4} {5}, ".format(tbCol["COLUMN_NAME"][i], new_col_type, new_col_length,  null_able, set_default, set_prikey )
        del null_able,set_default,set_prikey

    if attr.strip()[-1] == ",":
        attr = attr.strip()[:-1]
    # 生成 create table sql
    createSQL="IF OBJECT_ID(N'dbo.{0}', N'U') IS NULL CREATE TABLE {1} ({2})".format(t, t, attr)
    ms_cursor.execute(createSQL)
    ms_conn.commit()
    print("%s 表已创建." % t)

    # 迁移数据
    limit = 1000
    offset = 0
    while True:
        query = "select * from {} limit {} offset {}".format(t, limit, offset)
        df = pd.read_sql(query, mysql_conn)
        df = df.apply(lambda x: pd.to_datetime(x).dt.strftime('%Y-%m-%d %H:%M:%S') if x.dtype == 'datetime64[ns]' else x)
        # print(df)

        if df.empty:
            break

        df.to_sql(t, MSEngine, if_exists='append', index=False)

        # 更新偏移量
        offset += limit
    print("%s 表数据导入完成." % t)

mysql_conn.close()
ms_conn.close()
