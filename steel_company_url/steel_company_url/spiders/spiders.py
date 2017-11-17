#! coding: utf-8

import requests
import os
import pymongo
from pymongo import MongoClient
import datetime
import logging
import scrapy
import re
import time
import traceback
from scrapy import log
import sys
from scrapy.conf import settings
from steel_company_url.items import SteelCompanyUrlItem
from scrapy.selector import Selector
from lxml import etree
from ipdb import set_trace


reload(sys)
sys.setdefaultencoding('utf-8')

class SteelCompanyUrlSpider(scrapy.Spider):
    name = "spider"

    def __init__(self, settings, *args, **kwargs):
        super(SteelCompanyUrlSpider, self).__init__(*args, **kwargs)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    ## 网站url不变，但内容变，应采用post方式抓取，但通过分析，Form Data表单，发现每页的url可以通过From Data的view source 拼接成。
    ## url:http://e.mysteel.com/huangye/List，view source ：page=%d&mainIndustry=&city=&companyType=&keyWord=&isGXT=&isTop100=
    ## 中间加个 ？就可以了，page=后面的数字代表当前页数
    def start_requests(self):
        try:
            for page in range(1,9940):
                start_url = 'http://e.mysteel.com/huangye/List?page=%d&mainIndustry=&city=&companyType=&keyWord=&isGXT=&isTop100=' % (page)
                meta = {'dont_redirect': True, 'item': page, 'dont_retry': True}
                yield scrapy.Request(url=start_url, meta=meta, callback=self.parse, dont_filter=True)
        except:
            self.log('start_request error! (%s)' % (str(traceback.format_exc())), level=log.INFO)

    ## 遍历公司解析公司url
    def parse(self, response):
        sel = Selector(response)

        i = SteelCompanyUrlItem()

        if response.status != 200:
            self.log('fetch failed! status=%d, page=%d ' % (response.status, response.meta['item']), level=log.WARNING)

        urls = sel.xpath("//div[@class='companyTil']/a")
        for url in urls:
            i['company_url'] = url.xpath("@href")[0].extract()
            self.log('spider succesd! url=%s' % (i['company_url']), level=log.INFO)

            yield i

