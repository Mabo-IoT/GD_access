import pyodbc


class DB():
    # 程序最先初始化，遍历报警信息表
    # 只抓取最新一条信息进行判断
    def __init__(self):
        self.warning_list = []
        self.cursor = self.connect()
        self.finallID = None
        self.sql = 'select * from 报警资料 order by ID '


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
            # print('running') #TODO 测试无限循环
            self.catchFinall()


    def catchFinall(self):
        # 抓取报警信息表里最后一条信息并判断
        sql = 'select * from 报警资料 where id=(select max(id) from 报警资料);'
        self.cursor.execute(sql)
        row = self.cursor.fetchone()
        if row[0] > self.finallID:
            self.check(row)
            # 下面4行写在此if语句下面，不会一直刷新warning，只会在报警资料表里更新了信息的时候print
            if not self.warning_list == []:
                self.warning()
            else:
                print('NO WARNING')
        # TODO 如果想一直刷新warning信息，则取消下面4行注释并注释上面4行。！！！下面4行仅作为测试使用
        # if not self.warning_list == []:
        #     self.warning()
        # else:
        #     print('NO WARNING')


    def init(self):
        # 初始化遍历table里每一条信息
        self.cursor.execute(self.sql)
        while True:
            row = self.cursor.fetchone()
            if not row == None:
                self.check(row)
                self.finallID = row[0]
            else:
                break
        if not self.warning_list == []:
            self.warning()
        else:
            print('NO WARNING')


    def check(self,row):
        # 如果状态为产生，则将 ID 加入warning_list，如果状态为消除，从warning_list中移除
        if row[2] == '产生':
            id_warningString = (row[0],row[3])
            self.warning_list.append(id_warningString)
        elif row[2] == '消除':
            for i in self.warning_list:
                if row[3] == i[1]:
                    self.warning_list.remove(i)


    # 发送报警信息
    def warning(self):
        for i in self.warning_list:
            id = i[0]
            sql = 'select * from 报警资料 where id = {0}'.format(id)
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
            print('    WARNING！    ',row[3],'  备注信息:',row[4],'  时间:',row[1])





def main():
    db = DB()
    db.run()


if __name__ == '__main__':
    main()