# -*- coding: utf-8 -*-
import time
import pendulum
import traceback
import pyodbc
from logging import getLogger

from Doctopus.Doctopus_main import Check, Handler


log = getLogger('Doctopus.plugins')


def table_name_transform(table_name):
    
    table_names = {
        "测试数据": "test",
        "报警资料": "alarm",
        "日志信息": "log",
    }
    return table_names[table_name]


class MyCheck(Check):
    def __init__(self, configuration):
        super(MyCheck, self).__init__(configuration=configuration)
        self.conf = configuration['user_conf']['check']

        self.dbq= self.conf['DBQ']
        self.driver = self.conf['DRIVER'] 
        self.table_names = self.conf['table_names']
        # self.alarm_names = set()
        self.conn = self.connect(dbq=self.dbq, driver=self.driver)
        self.cursor = self.conn.cursor()

        self.test_data_id = None

    def connect(self, dbq, driver):
        """
        
        connect to access and create a cursor
        """
        word = r'DRIVER={0};DBQ={1};'.format(driver, dbq)
        log.debug(word)
        while True:
            try:
                conn_str = (
                   word
                )
                cnxn = pyodbc.connect(conn_str)
                cnxn.setencoding(encoding='utf-16le')

                if cnxn:
                    break

            except Exception as e:
                traceback.print_exc()
                log.error(e)
                log.info("please check if database and trying to connect again!")
        
        return cnxn

    def re_connect(self): # todo
        """

        reconnect to database
        :return:
        """
        self.cursor.close()
        self.conn.close()
        self.conn = self.connect(dbq=self.dbq, driver=self.driver)
        self.cursor = self.conn.cursor()

    def select_row(self, table_name):
        """
        select table_name row data
        """
        # select last data according to id
        sql = 'select * from {} order by ID desc'.format(table_name)
        self.cursor.execute(sql)
        row = self.cursor.fetchone()
        return row

    def check_valid(self, Id):
        """
        check last test data if valid
        """
        if self.test_data_id:
            if self.test_data_id == Id:
                return False
            else:
                self.test_data_id = Id
                return True
        else:
            self.test_data_id = Id
            return True

    def maketime_stamp(self, time_data):
        """
        parse time to timestamp
        """
        time_data = (int(time_data[0:4]),int(time_data[5:7]),int(time_data[8:10]),int(time_data[11:13]),int(time_data[14:16]),int(time_data[17:19]),0,0,0)
        log.debug("========================================")
        log.debug(time_data)
        date_timestamp = int(time.mktime(time_data)) * 1000000 # to fit infuxldb timestamp 'us'
        log.debug(date_timestamp)
        return date_timestamp

    def transform_warning(self, warning_state):
        
        warning = {
            "消除": 0,
            "产生": 1,
        }
        return warning[warning_state]

    def process_alarm_data(self, data):
        """
        parse alarm data to right format which influxdb fields need
        """
        _, time, warning_state, warning_message, remark = data

        date_timestamp = self.maketime_stamp(time)
        warning = self.transform_warning(warning_state)
        
        data = {
            "warning": warning,
            "wanring_string": warning_message, 
            "date_timestamp": date_timestamp,
        }

        return data

    def process_log_data(self, data):
        pass

    def process_test_data(self, data):
        """
        parse test data to right format which influxdb fields need
        """
        log.debug(data)
        Id, test, start_time, product_name, product_model, product_matrial, test_human, product, serial_num, sample_num, sample_state,voltage, perssure, current, flow, speed, pulsation, efficiency, turn_flow = data

        if self.check_valid(Id):
            data = {
                "date_timestamp": int(pendulum.now().float_timestamp * 1000000),
                "voltage": voltage,
                "pressure": perssure,
                "current": current,
                "flow": flow,
                "speed": speed,
                "pulsation": pulsation,
                "efficiency": efficiency,
                "turn_flow": turn_flow,
            }

            return data

        else:
            return None

    def handle_data_method(self, table_name):
        """
        handle data by table_name
        """
        methods = {
            '测试数据': self.process_test_data,
            '报警资料': self.process_alarm_data,
            '日志信息': self.process_log_data,
        }

        return methods[table_name]
    
    def add_table_name(self, data, table_name):
        """
        add table_name in fields
        """
        table_namme = table_name_transform(table_name)

        table_dict = {
            "table_name": table_name,
        }
        data.update(table_dict)

        return data

    def user_check(self):
        """

        :param command: user defined parameter.
        :return: the data you requested.
        """
        data_handle = None

        for table_name in self.table_names:
            try:
                data = self.select_row(table_name)    
                # when table have data
                if data:

                    data_handle = self.handle_data_method(table_name)(data)
                    
                    if data_handle:
                        data_handle = self.add_table_name(data_handle, table_name)

                else:
                    log.info("{} have no data".format(table_name))
                    data_handle = None
            
            except Exception as e:
                traceback.print_exc()
                log.error(e)
                self.re_connect()

            if data_handle:
                yield data_handle


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
        # extract date_timestamp
        timestamp = raw_data['date_timestamp']
        raw_data.pop('date_timestamp')
        # extract table_name
        table_name = table_name_transform(raw_data['table_name']) 
        table_name = '{0}_{1}'.format(self.table_name, table_name)
        log.debug(table_name)
        raw_data.pop('table_name')

        data_value_list = raw_data

        # user 可以在handle里自己按数据格式制定tags
        user_postprocessed = {'data_value': data_value_list,
                              'timestamp': timestamp,
                              'table_name': table_name,
                                }
        yield user_postprocessed
