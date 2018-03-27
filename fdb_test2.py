import firebirdsql

# The server is named 'bison'; the database file is at 'D:\mabo-yummy\GD_access\EKP.fdb'.
con = firebirdsql.connect(dsn='bison:/temp/test.fdb', user='sysdba', password='pass')

# Or, equivalently:
con = firebirdsql.connect(
    host='bison', database='/temp/test.fdb',
    user='sysdba', password='pass'
  )