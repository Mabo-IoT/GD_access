#引入firebirdsql库，可用pip安装，pip install firebirdsql
import firebirdsql as fdb
# SELECT * FROM MyTable;
# The server is named 'bison'; the database file is at 'D:\mabo-yummy\GD_access\EKP.fdb'.
# 服务器名字，此处本机为‘localhost’
# 默认user为sysdba，password为masterkey
con = fdb.connect(
    # dsn='bison:/temp/test.fdb','D:/mabo-yummy/GD_access/EKP.FDB''D:/testfdb/TEST.fdb'
    dsn='localhost:D:/mabo-yummy/GD_access/EKP.FDB',
    user='sysdba', password='masterkey',
    charset='GBK' # specify a character set for the connection
  )
# sql = 'SELECT * FROM CONTROL;'
sql = 'SELECT * FROM HISTALARM;'
# print(con)
cur = con.cursor()
data = cur.execute(sql)
# print(data.fetchall())
for i in data:
    print(i)
# while True:
#     row = cur.fetchone()
#     if not row:
#         break
#     print(row)