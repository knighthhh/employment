import datetime
import json
import threading
from time import sleep
import multiprocessing
import config
import re
import time
import redis
import html as HT
from lxml import etree
from lxml.etree import HTML
from download import Download
from db import MongoClient, MysqlClient, RedisClient

SHI_URL = 'https://www.zhipin.com/{shi_id}/'
QU_URL = 'https://www.zhipin.com/{shi_id}/{qu_id}/?ka=sel-business-1'
POSITION_URL = 'https://www.zhipin.com/c{pid}-p{zhiwei_id}/{zhen_id}/?page={pageToken}&sort=2&ka=page-{pageToken}'
NEW_POSITION_URL = 'https://www.zhipin.com/c{pid}-p{zhiwei_id}/?page={pageToken}&sort=2&ka=page-{pageToken}'

class Scheduler(object):
    def __init__(self):
        self.download = Download()
        self.db = MysqlClient()
        self.redisClient = RedisClient()

    def run(self):
        #self.get_qu()
        #self.get_zhen()
        # self.push_url_to_redis()
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
        redis_results = self.redisClient.pop('employment')
        try:
            json_obj = json.loads(redis_results[1].decode('utf8'))
        except:
            return None

        if json_obj:
            flag = True
            pageToken = 1

            #处理翻页问题
            while flag:
                detail_url_list = []
                url = json_obj['url']
                pre_page = re.search('\/\?page=(.*?)&', url).group(1)
                if int(pre_page) > 10:
                    break
                url = url.replace('page='+pre_page+'&sort=2&ka=page-'+pre_page, 'page=' + str(pageToken) + '&sort=2&ka=page-' + str(pageToken))
                cityId = json_obj['cityId']
                zhiweiId = json_obj['zhiweiId']
                print(url)
                html = self.download.get_html(url)

                if html is not None and html.status_code == 200:
                    html = HTML(html.text)

                    #判断是否是当天发布，是的话请求详情页, 判断数据库是否有这条数据，有的话不请求（暂时）
                    li_xpath = html.xpath('//div[@class="job-list"]/ul/li')
                    for li in li_xpath:
                        content = etree.tostring(li)
                        content = HT.unescape(content.decode())
                        content = HTML(content)
                        li_time = content.xpath('string(//div[@class="info-publis"]/p)')
                        href_url = content.xpath('string(//div[@class="info-primary"]//h3/a/@href)')
                        try:
                            last_str = li_time.split('发布于')[1]
                            minute = last_str.split(':')[1]
                            #判断是否当天发布
                            if minute:
                                #判断数据库存不存在：
                                try:
                                    cid = re.match('^/job_detail/(.*?)\.html', href_url).group(1)
                                    sql = "select * from positions where cid='%s'" %(cid)
                                    find_one_res = self.db.find_one(sql)
                                    if find_one_res is None:
                                        #先把cid插入，避免重复抓取
                                        sql = "insert into positions(cid) values ('%s')" %(cid)
                                        self.db.save(sql)
                                        detail_url_list.append(config.HOST_URL + href_url)
                                    else:
                                        print('数据库存在该记录：' + str(cid))
                                except:
                                    print('查询数据库出错：' + str(cid))
                        except:
                            print('该URL发布日期小于当天：' + config.HOST_URL + href_url)

                    results = self.get_detail(detail_url_list, cityId, zhiweiId)

                    #判断是否翻页
                    try:
                        last_li = html.xpath('string(//div[@class="job-list"]/ul/li[last()]//div[@class="info-publis"]/p)')
                        last_str = last_li.split('发布于')[1]
                        minute = last_str.split(':')[1]
                        if minute:
                            pageToken = str(int(pageToken) + 1)
                    except:
                        flag = False

                else:
                    print('该url无数据')

    def get_detail(self, detail_url_list, cityId, zhiweiId):
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
                posterUrl = html.xpath('string(//div[@class="detail-figure"]/img/@src)')
                content = html.xpath('string(//div[@class="job-sec"]/div[@class="text"])').strip()

                try:
                    company_text = html.xpath('string(//a[@ka="job-cominfo"]/@href)')
                    companyID = re.match('/gongsi/(.*?)\.html', company_text).group(1)
                except:
                    companyID = None
                createDate = int(time.time())

                #判断是否是当天发布
                temp_time = time.localtime(int(time.time()))
                now_DateStr = time.strftime("%Y-%m-%d", temp_time)
                lt = time.strptime(now_DateStr, "%Y-%m-%d")
                now_timestamp = int(time.mktime(lt))
                if publishDate == None or publishDate < now_timestamp or publishDate >= (now_timestamp + 86400):
                    print('特例.该url不是当天发布：' + str(url))
                    continue

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
                    'createDate': createDate,
                    'cityId': cityId,
                    'zhiweiId': zhiweiId
                }
                print(res_obj)
                sql = "insert into positions(cid,title,url,publishDate,publishDateStr,city,jingyan,xueli,price,posterName,posterId,posterUrl,content,companyID,createDate,cityId, zhiweiId)" \
                      " VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" \
                      % (cid,title,url,publishDate,publishDateStr,city,jingyan,xueli,price,posterName,posterId,posterUrl,content,companyID,createDate,cityId, zhiweiId)\
                      + "ON DUPLICATE KEY UPDATE title='%s', url='%s', publishDate='%s', publishDateStr='%s', city='%s', jingyan='%s', xueli='%s', price='%s', posterName='%s', posterId='%s', posterUrl='%s', content='%s', companyID='%s', createDate='%s',cityId='%s', zhiweiId='%s'" \
                      %(title,url,publishDate,publishDateStr,city,jingyan,xueli,price,posterName,posterId,posterUrl,content,companyID,createDate,cityId, zhiweiId)
                self.db.save(sql)
                return res_obj
            else:
                print('请求详情页失败：' + str(url))

    def push_url_to_redis(self):
        # zhiwei_list = []
        # zhiwei_sql = 'select * from zhiwei'
        # zhiwei_results = self.db.find_all(zhiwei_sql)
        # for zhiwei in zhiwei_results:
        #     zhiwei_list.append(zhiwei[2])
        #
        # zhen_sql = 'select * from zhen'
        # zhen_results = self.db.find_all(zhen_sql)
        #
        # for res in zhen_results:
        #     pid = res[1]
        #     zhen_id = res[2]
        #     for zhiwei_id in zhiwei_list:
        #         url = POSITION_URL.format(pid=pid, zhen_id=zhen_id, zhiwei_id=zhiwei_id, pageToken='1')
        #         self.redisClient.push('employment',url)


        zhiwei_list = []
        zhiwei_sql = 'select * from zhiwei'
        zhiwei_results = self.db.find_all(zhiwei_sql)
        for zhiwei in zhiwei_results:
            zhiwei_list.append(zhiwei[2])

        shi_sql = 'select * from shi'
        shi_results = self.db.find_all(shi_sql)

        for res in shi_results:
            pid = res[2]
            for zhiwei_id in zhiwei_list:
                url = NEW_POSITION_URL.format(pid=pid, zhiwei_id=zhiwei_id, pageToken='1')
                url_obj = {"url":url, "cityId":pid, "zhiweiId":zhiwei_id}
                self.redisClient.push('employment', json.dumps(url_obj))