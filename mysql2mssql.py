# -*- coding: UTF-8 -*-

from connector import *


# pymssql.Binary = bytearray

# 需要转换的数据库
converDB=""

# 字段类型转换
colConverDict = {
    "varchar":"nvarchar",
    "text":"nvarchar",
    "longtext":"nvarchar",
    "mediumtext":"nvarchar",
    "datetime":"datetime2",
    "tinyint":"smallint",
    "longblob":"varbinary(max)",
    "mediumblob":"varbinary(max)",
    "timestamp":"smalldatetime",
    "double":"float"
}

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

with MSSQLManager("true") as msAutoCommit:
    createDBSQL= "IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = '{}') CREATE DATABASE [{}]".format(converDB, converDB)

    print(createDBSQL)
    print(type(createDBSQL))
    msAutoCommit.moddify(createDBSQL)

with MYSQLManager() as my, MYSQLManager("str") as myStr, MSSQLManager() as ms:
    # 获取所有表名
    myTbs=my.get_list("select TABLE_NAME from information_schema.TABLES where TABLE_SCHEMA='%s' and TABLE_TYPE='BASE TABLE'" % converDB)

    limit = 1000
    offset = 0

    for tbs in myTbs:
        t=tbs["TABLE_NAME"]
        tbCol=my.get_list("SELECT * FROM information_schema.COLUMNS where TABLE_SCHEMA='{0}' and TABLE_NAME='{1}' ORDER BY ORDINAL_POSITION".format(converDB, t))

        attr=""
        for i in range(len(tbCol)):
            
            # 判断字段类型是否可以为空
            if tbCol[i]["IS_NULLABLE"] != "YES":
                null_able="NOT NULL"
            else:
                null_able=""

            # 设置字段默认值
            col_default=tbCol[i]["COLUMN_DEFAULT"] #.replace("None","NULL").replace("None", "NULL")
            if str(col_default) == 'None':
                set_default="DEFAULT" + " " + str(tbCol[i]["COLUMN_DEFAULT"])
            else:
                set_default="DEFAULT" + " '" + str(tbCol[i]["COLUMN_DEFAULT"]) + "'"
            set_default=set_default.replace("None","NULL").replace("None","NULL")

            #if tbCol[i]["COLUMN_KEY"] == "PRI":
            #    set_prikey="PRIMARY KEY"
            #else:
            #    set_prikey=""

            # 设置字段类型和字段长度
            new_col_type, new_col_length=converCol(tbCol[i]["COLUMN_TYPE"])

            #attr += "[" + tbCol[i]["COLUMN_NAME"] + "] " + tbCol[i]["COLUMN_TYPE"] + " " + set_prikey + " " + null_able + set_default + ","
            attr += "[{0}] {1}{2} {3} {4}, ".format(tbCol[i]["COLUMN_NAME"], new_col_type, new_col_length,  null_able, set_default)
            del null_able,set_default

        if attr.strip()[-1] == ",":
            attr = attr.strip()[:-1]
        # 生成 create table sql
        createSQL="IF OBJECT_ID(N'dbo.{0}', N'U') IS NULL CREATE TABLE {1} ({2})".format(t, t, attr)
        # print(createSQL)
        ms.moddify(createSQL)
        print("%s 表已创建." % t)

        # 循环处理批量插入数据
        while True:
            query = "select * from {} limit {} offset {}".format(t, limit, offset)
            result= myStr.get_list(query)

            if not result:
                break
            
            insert_data=str(result).replace("None","NULL")
            insert_data=insert_data[1:-1]
            insertSQL="INSERT INTO {} values {}".format(t, insert_data)
            # print(insertSQL)
            ms.moddify(insertSQL)

            # 更新偏移量
            offset += limit
        print("%s 表数据导入完成." % t)
