import firebirdsql

# The server is named 'bison'; the database file is at 'D:\mabo-yummy\GD_access\EKP.fdb'.
con = firebirdsql.connect(
    # dsn='bison:/temp/test.fdb',
    dsn='localhost:D:\mabo-yummy\GD_access\EKP.fdb',
    user='sysdba', password='masterkey',
    # charset='UTF8' # specify a character set for the connection
  )