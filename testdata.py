# __Yummy__ Access 数据库获取‘测试数据’表中的测试信息

# 其中各种测试数据的'变量名'定义,及所在列，如下：

# V/11    P/12    I/13    F/14    RS/15     PF/16      E/17     HF/18
# 电压    压力    电流    流量     转速    压力脉动    效率    百转流量
#TODO 状态为空的时候未考虑到


import pyodbc


class DB():
    # 程序最先初始化，遍历报警资料表
    # 只抓取最新一条信息进行判断

    def __init__(self):
        self.cursor = self.connect()
        self.finallID = None
        self.sql ='select * from 测试数据 order by ID '


    def connect(self):
        # 连接数据库

        conn_str = (
            r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
            r'DBQ=D:\mabo-yummy/GD_access/PTDB1.accdb;'
        )
        cnxn = pyodbc.connect(conn_str)
        crsr = cnxn.cursor()
        cnxn.setencoding(encoding='utf-16le')
        return crsr


    def run(self):
        # 初始化后，进行抓取最后一条数据循环

        self.init()
        while 1:
            # print('running') # 测试无限循环是否正常跑着
            self.catchFinall()


    def getdata(self,row):
        ID = row[0]
        V = row[11]
        P = row[12]
        I = row[13]
        F = row[14]
        RS = row[15]
        PF = row[16]
        E = row[17]
        HF = row[18]
        print(ID,V,P,I,F,RS,PF,E,HF) # TODO 此处print换成对数据执行操作的函数即可


    def catchFinall(self):
        # 抓取测试数据表里最后一条信息并判断

        sql = 'select * from 测试数据 where id=(select max(id) from 测试数据);'
        self.cursor.execute(sql)
        row = self.cursor.fetchone()
        if row[0] > self.finallID:
            self.finallID = row[0]
            # print('this is the last finallID',self.finallID) # 测试最后一条是否被抓取
            self.getdata(row)



    def init(self):
        # 初始化遍历table里每一条信息
        self.cursor.execute(self.sql)
        while True:
            row = self.cursor.fetchone()
            if not row == None:
                self.getdata(row)
                self.finallID = row[0]
            else:
                break








def main():
    db = DB()
    db.run()

if __name__ == '__main__':
    main()