# -*- coding: utf-8 -*-
"""
Created on Fri Jul 20 16:05:53 2018

@author: ldh
"""

# main.py

from threading import Lock
from spider_base import SpiderMaster
from spiders import THSSpider,ZZWSpdier,CNSTOCKSpider,STCNSpider,PeopleSpider

if __name__ == '__main__':
    spider_master = SpiderMaster()
    lock = Lock()
    
    spider_list = []
    # 创建各个网站的spider
    spider_list.append(THSSpider(lock))
    spider_list.append(ZZWSpdier(lock))
    spider_list.append(CNSTOCKSpider(lock))
    spider_list.append(STCNSpider(lock))
    spider_list.append(PeopleSpider(lock))
    
    for each in spider_list:
        spider_master.add_spider(each)
    spider_master.run()