import firebirdsql
conn = firebirdsql.connect(
    host='localhost',
    # database='/foo/bar.fdb',
    database='/EKP.fdb',
    port=3050,
    user='alice',
    password='secret'
)
cur = conn.cursor()
cur.execute("select * from baz")
for c in cur.fetchall():
    print(c)
conn.close()