import firebirdsql

conn = firebirdsql.connect(
    host='localhost',
    database='D:\mabo-yummy\GD_access/EKP.fdb',
    port=3050,
    user='alice',
    password='secret'
)
cur = conn.cursor()
cur.execute("select * from 日志信息")
for c in cur.fetchall():
    print(c)
conn.close()