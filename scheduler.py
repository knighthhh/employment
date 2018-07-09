import datetime
import json
import threading
from time import sleep

import config
import re
from lxml.etree import HTML
from download import Download
from db import MongoClient, MysqlClient

SHI_URL = 'https://www.zhipin.com/{shi_id}/'
QU_URL = 'https://www.zhipin.com/{shi_id}/{qu_id}/?ka=sel-business-1'

class Scheduler(object):
    def __init__(self):
        self.download = Download()
        self.db = MysqlClient()

    def run(self):
        #self.get_qu()
        self.get_zhen()

    def get_qu(self):
        sql = 'select * from shi'
        results = self.db.find_all(sql)
        for res in results:
            shi_id = res[2]
            url = SHI_URL.format(shi_id='c'+shi_id)
            print(url)
            html = self.download.get_html(url)
            if html.status_code == 200 and html is not None:
                html = HTML(html.text)
                qu_id_list = html.xpath('//dl[@class="condition-district show-condition-district"]/dd/a/@href')
                qu_name_list = html.xpath('//dl[@class="condition-district show-condition-district"]/dd/a/text()')
                for qu_id, name in zip(qu_id_list[1:], qu_name_list[1:]):
                    qu_id = qu_id.split('/')
                    qu_id = qu_id[2]
                    sql = '''insert into qu(pid,qu_id,name) VALUES ('{pid}','{qu_id}','{name}')'''\
                        .format(pid=shi_id,qu_id=qu_id, name=name)
                    print(sql)
                    self.db.save(sql)
            else:
                print('该url无数据')

    def get_zhen(self):
        sql = 'select * from qu'
        results = self.db.find_all(sql)
        for res in results:
            shi_id = res[1]
            qu_id = res[2]
            url = QU_URL.format(shi_id='c'+shi_id, qu_id=qu_id)
            print(url) 
            html = self.download.get_html(url)
            if html is not None and html.status_code == 200:
                html = HTML(html.text)
                zhen_id_list = html.xpath('//dl[@class="condition-area show-condition-area"]/dd/a/@href')
                zhen_name_list = html.xpath('//dl[@class="condition-area show-condition-area"]/dd/a/text()')
                for zhen_id, name in zip(zhen_id_list[1:], zhen_name_list[1:]):
                    zhen_id = zhen_id.split('/')
                    zhen_id = zhen_id[2]
                    sql = '''insert into zhen(pid,qu_id, zhen_id,name) VALUES ('{pid}','{qu_id}','{zhen_id}','{name}')'''\
                        .format(pid=shi_id,qu_id=qu_id,zhen_id=zhen_id, name=name)
                    print(sql)
                    self.db.save(sql)
            else:
                print('该url无数据')
