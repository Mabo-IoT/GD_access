# -*- coding: utf-8 -*-
import time
import pyodbc
from logging import getLogger

from Doctopus.Doctopus_main import Check, Handler

log = getLogger('Doctopus.plugins')


class MyCheck(Check):
    def __init__(self, configuration):
        super(MyCheck, self).__init__(configuration=configuration)
        self.conf = configuration['user_conf']['check']
        self.testdata_names = set()
        self.cnxn = self.connect()

        self.cursor = self.cnxn.cursor()

    def connect(self, path='../PTDB1.accdb', host='localhost'):
        """

        connect to access and create a cursor
        """
        log.debug(path)
        while True:
            try:
                conn_str = (
                    r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
                    r'DBQ=D:\mabo-yummy/GD_access/PTDB1.accdb;'
                )
                cnxn = pyodbc.connect(conn_str)
                cnxn.setencoding(encoding='utf-16le')

                if cnxn:
                    break

            except Exception as e:
                log.error(e)
                log.info("please check if database and trying to connect again!")

            return cnxn

    def re_connect(self): # todo
        """

        reconnect to database
        :return:
        """
        pass

    def select_testdata_row(self,table_name="测试数据"): # todo 此表没有时间，influxdb里的时间怎么算
        """

        select 测试数据 testdata
        """
        sql = 'select * from {} order by ID'
        sql = sql.format(table_name)
        self.cursor.execute(sql)
        row = self.cursor.fetchone()
        row = [row[0], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18]]
        # print(row)
        return row

    def process_testdata_data(self,testdata_data):
        """

        parse 测试数据 testdata to right format which influxdb fields need
        """
        # unpack testdata_data
        V, P, I, F, RS, PF, E, HF = testdata_data
        # datetime to timestamp

        # for i in self.warning_list:
        #     id = i[0]
        #     sql = 'select * from 报警资料 where id = {0}'.format(id)
        #     self.cursor.execute(sql)
        #     row = self.cursor.fetchone()

        # time1 = row[1]
        # time1 = (int(time1[0:4]),int(time1[5:7]),int(time1[8:10]),int(time1[11:13]),int(time1[14:16]),int(time1[17:19]),0,0,0)
        # date_timestamp = time.mktime(warning_time) # to fit infuxldb timestamp 'us'

        data = {
            # "date_timestamp": date_timestamp,
            "V": V,
            "P": P,
            "I": I,
            "F": F,
            "RS": RS,
            "PF": PF,
            "E": E,
            "HF": HF
        }
        return data

    def user_check(self):
        """

        :param command: user defined parameter.
        :return: the data you requested.
        """


        # select data from access database
        testdata_data = self.select_testdata_row()
        # process select data to right fields which influxdb need
        testdata_data_handle = self.process_testdata_data(testdata_data)

        data = 'check的data'
        log.debug('%s', data)
        time.sleep(2)
        yield data


class MyHandler(Handler):
    def __init__(self, configuration):
        super(MyHandler, self).__init__(configuration=configuration)

    def user_handle(self, raw_data):
        """
        用户须输出一个dict，可以填写一下键值，也可以不填写
        timestamp， 从数据中处理得到的时间戳（整形?）
        tags, 根据数据得到的tag
        data_value 数据拼接形成的 list 或者 dict，如果为 list，则上层框架
         对 list 与 field_name_list 自动组合；如果为 dict，则不处理，认为该数据
         已经指定表名
        measurement 根据数据类型得到的 influxdb表名

        e.g:
        list:
        {'data_value':[list] , required
        'tags':[dict],        optional
        'table_name',[str]   optional
        'timestamp',int}      optional

        dict：
        {'data_value':{'fieldname': value} , required
        'tags':[dict],        optional
        'table_name',[str]   optional
        'timestamp',int}      optional

        :param raw_data:
        :return:
        """
        # exmple.
        # 数据经过处理之后生成 value_list
        log.debug('%s', raw_data)
        data_value_list = [raw_data]

        tags = {'user_defined_tag': 'data_ralated_tag'}

        # user 可以在handle里自己按数据格式制定tags
        user_postprocessed = {'data_value': data_value_list,
                              'tags': tags, }
        yield user_postprocessed
