import pyodbc


class DB:
    def __init__(self):
        self.cursor = self.connect()
        self.sql = 'select * from 日志信息 order by ID '
        # self.sql = 'SELECT Name FROM Master.SysDatabases'

    def connect(self):
        conn_str = (
           r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
           r'DBQ=D:\mabo-yummy/GD_access/PTDB1.accdb;'
        )
        cnxn = pyodbc.connect(conn_str)
        crsr = cnxn.cursor()
        for table_info in crsr.tables(tableType='TABLE'):
            print(table_info)
            print(table_info.table_name)
        
        cnxn.setencoding(encoding='utf-16le')
        return crsr
    # TODO 此处的游标返回的值检查是否符合时间序列

    def run(self):

        self.cursor.execute(self.sql)
        while True:
            row = self.cursor.fetchone()
            if not row:
                break
            print(row)

            # data = self.cursor
            # # rows = self.cursor.fetchall()
            # # data = [d for d in rows]
            # for d in data:
            #     print(data)

def main():
    db = DB()
    db.run()

if __name__ == '__main__':
    main()
