
# -*- coding: utf-8 -*-
##
##
# @file spider_steel.py
# @brief spider steel company info
# @author kangchch
# @version 1.0
# @date 2017-11-16


from scrapy.http import Request
import xml.etree.ElementTree
from scrapy.selector import Selector

import scrapy
import re
from pymongo import MongoClient
from copy import copy
import traceback
import pymongo
from scrapy import log
from steel_company_info.items import SteelCompanyInfoItem
import time
import datetime
import sys
import logging
import random
import binascii
from scrapy.conf import settings
import json

reload(sys)
sys.setdefaultencoding('utf-8')


class SteelCompanyInfoSpider(scrapy.Spider):
    name = "spider_steel"

    def __init__(self, settings, *args, **kwargs):
        super(SteelCompanyInfoSpider, self).__init__(*args, **kwargs)
        self.settings = settings
        mongo_info = settings.get('MONGO_INFO', {})

        try:
            self.mongo_db = pymongo.MongoClient(mongo_info['host'], mongo_info['port']).steel_info
        except Exception, e:
            self.log('connect mongo 192.168.60.65:10010 failed! (%s)' % (str(e)), level=log.CRITICAL)
            raise scrapy.exceptions.CloseSpider('initialization mongo error (%s)' % (str(e)))

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def start_requests(self):

        try:
            records = self.mongo_db.company_url.find({'status': 0}, {'company_url': 1})
            for record in records:
                company_url = record['company_url']
                meta = {'dont_redirect': True, 'company_url': company_url, 'dont_retry': True}
                self.log('spider new url=%s' % (company_url), level=log.INFO)
                yield scrapy.Request(url = company_url, meta = meta, callback = self.parse_contact_page, dont_filter = True)
            # company_url = 'http://e.mysteel.com/ID2280188'
            # meta = {'dont_redirect': True, 'company_url': company_url, 'dont_retry': True}
            # self.log('spider new url=%s' % (company_url), level=log.INFO)
            # yield scrapy.Request(url = company_url, meta = meta, callback = self.parse_contact_page, dont_filter = True)
        except:
            self.log('start_request error! (%s)' % (str(traceback.format_exc())), level=log.INFO)

    # 解析公司联系方式和简介
    def parse_contact_page(self, response):
        sel = Selector(response)

        ret_item = SteelCompanyInfoItem()
        ret_item['update_item'] = {}
        i = ret_item['update_item']
        i['company_url'] = response.meta['company_url']

        if response.status != 200 or len(response.body) <= 0:
            self.log('fetch failed ! status = %d, url=%s' % (response.status, i['company_url']), level = log.WARNING)

        ## introduce 公司简介
        introduce = sel.xpath("//div[@class='aboutCon']")
        i['introduce'] = '' if not introduce else introduce[0].xpath('string(.)').extract()[0].strip().replace('\r\n','').replace(' ','')
        # print i['introduce']

        ## company_name 公司名称
        company_name = re.findall('(?<=companyName=).*?(?=\")', response.body, re.S)
        i['company_name'] = '' if not company_name else company_name[0].strip()
        # print i['company_name']

        # telephone 公司电话
        telephone = re.findall("公司电话：\n?\s*([^\n]+)(?=<br)", response.body, re.S)
        # telephone = sel.xpath(u"//p[re:test(text(),'公司电话：')]//text()").extract()[0]
        i['telephone'] = '' if not telephone else telephone[0]
        # print i['telephone']

        # fax 公司传真
        fax = re.findall("公司传真：\n?\s*([^\n]+)(?=<br)", response.body, re.S)
        i['fax'] = '' if not fax else fax[0].strip()
        # print i['fax']

        # url 公司网址
        url = sel.xpath("//span[@id='cutWebsite']/text()").extract()
        i['url'] = '' if not url else url[0].strip()
        # print i['url']

        # address 地址
        address = sel.xpath("//span[@id='cutAddress']/text()").extract()
        i['address'] = '' if not address else address[0].strip().replace(' ', '')
        # print i['address']

        ## renzheng 认证情况
        renzheng = sel.xpath("//a[@class='#']/text()").extract()
        if renzheng:
            i['rz_year'] = '' if not renzheng else renzheng[0].strip()
            i['renzheng'] = u"已认证"
            meta = {'dont_redirect': True, 'item': ret_item, 'dont_retry': True}
            yield scrapy.Request(url = i['company_url'] + '/da', meta = meta, callback = self.parse_renzheng_page, dont_filter = True)
        else:
            i['renzheng'] = u"未认证"
            i['rz_year'] = ''
            i['registr'] = ''
            i['legal'] = ''
            i['company_type'] = ''
            i['operate_period'] = ''
            i['operate_range'] = ''
            i['found_date'] = ''
            i['register_office'] = ''
            i['register_addr'] = ''
            i['register_capital'] = ''
            self.log('未认证, url=%s ' % (i['company_url']), level=log.INFO)
            yield ret_item

    #解析认证信息
    def parse_renzheng_page(self, response):
        sel = Selector(response)

        ret_item = response.meta['item']
        i = ret_item['update_item']

        ## 注册号 registr
        registr = sel.xpath("//tr[3]/td[2]//text()").extract()
        i['registr'] = '' if not registr else registr[0].strip()
        # print i['registr']

        ## 法人代表 legal
        legal = sel.xpath("//tr[3]/td[4]//text()").extract()
        i['legal'] = '' if not legal else legal[0].strip()
        # print i['legal']

        ## 企业类型 company_type
        company_type = sel.xpath("//tr[4]/td[4]//text()").extract()
        i['company_type'] = '' if not company_type else company_type[0].strip()
        # print i['company_type']

        ## 经营期限 operate_period
        operate_period = sel.xpath("//tr[5]/td[2]//text()").extract()
        i['operate_period'] = '' if not operate_period else operate_period[0].strip()
        # print i['operate_period']

        ## 经营范围 operate_range
        operate_range = sel.xpath("//tr[5]/td[4]/div[@id='jyfw']//text()").extract()
        i['operate_range'] = '' if not operate_range else operate_range[0].strip()
        # print i['operate_range']

        ## 成立日期 found_date
        found_date = sel.xpath("//tr[2]/td[4]//text()").extract()
        i['found_date'] = '' if not found_date else found_date[0].strip()
        # print i['found_date']

        ## 登记机关 register_office 
        register_office = sel.xpath("//tr[4]/td[2]//text()").extract()
        i['register_office'] = '' if not register_office else register_office[0].strip()
        # print i['register_office']

        ## 注册地址 register_addr
        register_addr = sel.xpath("//tr[1]/td[4]//text()").extract()
        i['register_addr'] = '' if not register_addr else register_addr[0].strip()
        # print i['register_addr']

        ## 注册资本 register_capital
        register_capital = sel.xpath("//tr[2]/td[2]//text()").extract()
        i['register_capital'] = '' if not register_capital else register_capital[0].strip()
        # print i['register_capital']

        self.log(' . company_name:%s, url=%s ' % (i['company_name'], i['company_url']), level=log.INFO)
        yield ret_item

