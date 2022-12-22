# mysql2mssql

## 说明
这个工具是用来将 MySQL 迁移到 SQL Server.

## 字段类型对应转换
| MySQL | SQL Server |
| --- | --- |
| varchar | nvarchar |
| text | nvarchar |
| longtext | nvarchar |
| datetime | datetime2 |
| tinyint | smallint |
| longblob | varbinary |
| varchar | nvarchar |
| text | nvarchar |
| longtext | nvarchar |
| mediumtext | nvarchar |
| datetime | datetime2 |
| tinyint | smallint |
| longblob | varbinary(max) |
| mediumblob | varbinary(max) |
| timestamp | smalldatetime |
| double | float |