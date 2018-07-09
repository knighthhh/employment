import datetime
import json
import threading
from time import sleep

import config
import re
import time
from lxml.etree import HTML
from download import Download
from db import MongoClient, MysqlClient

SHI_URL = 'https://www.zhipin.com/{shi_id}/'
QU_URL = 'https://www.zhipin.com/{shi_id}/{qu_id}/?ka=sel-business-1'
POSITION_URL = 'https://www.zhipin.com/c{pid}-p{zhiwei_id}/{zhen_id}/?page={pageToken}&sort=2&ka=page-{pageToken}'

class Scheduler(object):
    def __init__(self):
        self.download = Download()
        self.db = MysqlClient()

    def run(self):
        #self.get_qu()
        #self.get_zhen()
        self.get_position()

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

    def get_position(self):
        zhiwei_list = []
        zhiwei_sql = 'select * from zhiwei'
        zhiwei_results = self.db.find_all(zhiwei_sql)
        for zhiwei in zhiwei_results:
            zhiwei_list.append(zhiwei[2])

        zhen_sql = 'select * from zhen'
        zhen_results = self.db.find_all(zhen_sql)

        for res in zhen_results:
            pid = res[1]
            zhen_id = res[2]
            for zhiwei_id in zhiwei_list:
                flag = True
                pageToken = 1
                #处理翻页问题
                while flag:
                    #self.next()
                    detail_url_list = []
                    url = POSITION_URL.format(pid=pid, zhen_id=zhen_id, zhiwei_id=zhiwei_id, pageToken=pageToken)
                    print(url)
                    html = self.download.get_html(url)

                    if html is not None and html.status_code == 200:
                        html = HTML(html.text)

                        xpath_list = html.xpath('//div[@class="job-list"]/ul/li//div[@class="info-primary"]//h3/a/@href')
                        for li in xpath_list:
                            detail_url_list.append(config.HOST_URL + li)

                        self.get_detail(detail_url_list)


                    else:
                        print('该url无数据')

    def get_detail(self, detail_url_list):
        for url in detail_url_list:
            print('下载该详情页：' + url)
            html = self.download.get_html(url)
            if html is not None and html.status_code == 200:
                html = HTML(html.text)

                try:
                    cid = re.match('^https://www.zhipin.com/job_detail/(.*?)\.html', url).group(1)
                except:
                    print('获取cid失败')
                    continue

                title = html.xpath('string(//h1)')
                url = url
                try:
                    publishDateStr = html.xpath('string(//span[@class="time"])').split('发布于')[1]
                    publishDate = int(time.mktime(time.strptime(publishDateStr, "%Y-%m-%d %H:%M")))
                except:
                    publishDateStr = None
                    publishDate = None

                try:
                    info = html.xpath('string(//div[@class="job-banner"]//div[@class="info-primary"]/p)')
                    info = info.split('：')
                    city = info[1][:-2]
                    jingyan = info[2][:-2]
                    xueli = info[3]
                except:
                    city = None
                    jingyan = None
                    xueli = None
                price = html.xpath('string(//div[@class="info-primary"]//span[@class="badge"])')
                posterName = html.xpath('string(//h2)')
                posterId = None
                posterUrl = html.xpath('//div[@class="detail-figure"]/img/@src')
                content = html.xpath('string(//div[@class="job-sec"]/div[@class="text"])')
                try:
                    company_text = html.xpath('//a[@ka="job-cominfo"]/@href')
                    companyID = re.match('/gongsi/(.*?)\.html',company_text)
                except:
                    companyID = None

                res_obj = {
                    'cid': cid,
                    'title': title,
                    'url': url,
                    'publishDateStr': publishDateStr,
                    'publishDate': publishDate,
                    'city': city,
                    'jingyan': jingyan,
                    'xueli': xueli,
                    'price': price,
                    'posterName': posterName,
                    'posterId': posterId,
                    'posterUrl': posterUrl,
                    'content': content,
                    'companyID': companyID,
                }
                print(res_obj)
                return res_obj
