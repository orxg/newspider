# -*- coding: utf-8 -*-
"""
Created on Fri Jul 20 14:57:43 2018

@author: ldh
"""

# spider_base.py

import time
import random
import threading
from sqlalchemy.types import VARCHAR,DATETIME,NVARCHAR,INT
import requests
import pandas as pd

from database_engine import DatabaseEngine
from SQLs import SQL_MERGE_MACRO_NEWS,SQL_MERGE_MACRO_NEWS_FOR_HEADER
from utils import header_generator


class NewsSpider(object):
    def __init__(self,spider_name,source_name,title_url,lock,
                 update_seconds = 600):
        '''
        新闻爬虫.
        
        Parameters
        ----------
        spider_name
            爬虫名称,英文
        source_name
            新闻源名称,unicode
        title_url
            标题页url
        lock
            线程共享锁,threading.lock()
        update_seconds
            int,基础更新间隔
        if_header
            int,0 或 1 是否头条爬虫
        '''
        self.spider_name = spider_name
        self.source_name = source_name
        self.title_url = title_url
        self.lock = lock
        self.update_seconds = update_seconds
        self.if_header = 0
        
    def init_spider(self,if_connect = True): 

        self.header_generator = header_generator
        
        if if_connect:
            self.engine_obj = DatabaseEngine('xiaoyi')
            self.engine = self.engine_obj.get_engine()
            self.session = self.engine_obj.get_session()
        
        self.additions = pd.DataFrame()
        self.spider_thread = threading.Thread(target = self.spider_data,
                                              name = self.spider_name)
        
        # 最近更新标记
        self.last_flag = None # 最近更新,用来进行增量判断,可以是时间等
        print 'Spider %s initilize succesfully'%self.spider_name 
        
    def spider_data(self):
        print 'Spider %s starts to run...'%self.spider_name
        while True:
            # 获取标题页
            status_code = self._get_titles_response()
            
            if status_code != 200:
                continue
            
            # ----------- 标题页分析,确定增量更新内容self.additions -----------
            # additions.columns = [title,href,news_time,content,news_source]
            # 保证additions都有上述的column,没有内容的用None
            self._parse_titles_response()
            # ------------------------------------------------------------------
            
            
            if len(self.additions) == 0:
                self._have_a_rest()
                continue
                
            for idx,row in self.additions.iterrows():
                href = row['href']
                status_code = self._get_content_response(href)
                if status_code != 200:
                    self._have_a_short_rest()
                    continue
                self._parse_content_response(idx)
                
            self._check_additions()
            
            if len(self.additions) == 0:
                self._have_a_rest()
                continue
            self._add_unique_flag()
            self._add_header_flag()
            self._write_into_db()
            self._have_a_rest()
            
    def _get_titles_response(self):
        '''
        获取标题页respsonse.
        '''
        self.titles_response = requests.get(self.title_url,headers = self.header_generator())
        return self.titles_response.status_code
    
    def _get_content_response(self,href):
        '''
        获取新闻具体内容response.
        '''
        try:
            self.content_response = requests.get(href,headers = self.header_generator())
            return self.content_response.status_code
        except Exception as e:
            print 'Spider {sp} failed to get content from {web} because: {error}'.format(
                    sp = self.spider_name,web = href,error = str(e))
            return 0
    
    def _parse_titles_response(self):
        '''
        分析标题页response,返回additions(增量更新内容).
        包括标题、新闻链接、时间(可选)、来源.
        '''
        pass
        
    def _parse_content_response(self,idx):
        '''
        解析内容页response.
        '''
        pass
    
    def _check_additions(self):
        '''
        写入数据库前对self.additions内容进行检测,确保没有问题.
        '''
        # 剔除title重复内容
        self.additions = self.additions.sort_values('news_time',
                                                    ascending = False)
        self.additions = self.additions.drop_duplicates(subset = 'title')
        
        # 剔除空内容
        self.additions = self.additions.dropna(subset = ['content'])
        
        # 剔除content数据量太少的记录
        self.additions['content_len'] = self.additions['content'].apply(lambda x: len(x))
        self.additions = self.additions.loc[self.additions['content_len'] > 3]
        
        self.additions = self.additions.drop('content_len',axis = 1)
        
    def _add_unique_flag(self):
        '''
        为新闻内容添加唯一性标志.实现新闻去重.
        '''
        punctuation_list = [u'?',u'.',u',',u'!',u':',u'"',u' ',u'…',u'%',u'<',u'>',
                           u'？', u'。',u'，',u'！',u'：',u'”',u'“',u'、',u'《',
                           u'》']
        def delete_punctuations(title):
            new_title = title
            for punc in punctuation_list:
                new_title = new_title.replace(punc,'')
            return new_title
        self.additions['unique_flag'] = self.additions['title'].apply(delete_punctuations)
        
    def _add_header_flag(self):
        '''
        为新闻内容添加是否头条的字段.
        '''
        self.additions.loc[:,'if_header'] = self.if_header
                
    def _write_into_db(self):
        '''
        写入数据库.
        '''

        self.lock.acquire() # 防止同一时间有其他进程对数据库进行写入
        self.additions.to_sql('news_macro_source',self.engine,
                              index = False,if_exists = 'replace',
                         dtype = {'title':VARCHAR(256),
                                  'news_time':VARCHAR(64),
                                  'href':VARCHAR(256),
                                  'content':NVARCHAR(),
                                  'update_datetime':DATETIME,
                                  'news_source':VARCHAR(32),
                                  'unique_flag':VARCHAR(256),
                                  'if_header':INT})
        if not self.if_header:
            self.session.execute(SQL_MERGE_MACRO_NEWS)
            self.session.commit()
        else:
            self.session.execute(SQL_MERGE_MACRO_NEWS_FOR_HEADER)
            self.session.commit()
        self.lock.release() # 释放
    
    def _have_a_rest(self):
        time.sleep(self.update_seconds + random.randint(0,60))
        
    def _have_a_short_rest(self):
        time.sleep(3)
        
    def start(self):
        self.spider_thread.start()
    
    def join(self):
        self.spider_thread.join()
    
    def test_spider(self,if_write = False):
        self.header_generator = header_generator
        self.last_flag = None
        self.additions = pd.DataFrame()
        status_code = self._get_titles_response()    
        print 'TITLE RESPONSE STATUS: %s'%(str(status_code))
        # ----------- 标题页分析,确定增量更新内容self.additions -----------
        # additions.columns = [title,href,news_time,content,news_source]
        # 保证additions都有上述的column,没有内容的用None
        self._parse_titles_response()
        # ------------------------------------------------------------------
            
        for idx,row in self.additions.iterrows():
            href = row['href']
            status_code = self._get_content_response(href)
            print 'CONTENT RESPONSE STATUS: %s'%(str(status_code))
            self._parse_content_response(idx)
            
        if if_write:
            self.engine_obj = DatabaseEngine('xiaoyi')
            self.engine = self.engine_obj.get_engine()
            self.session = self.engine_obj.get_session()
            self.additions.to_sql('news_macro_source',self.engine,
                                  index = False,if_exists = 'replace',
                             dtype = {'title':VARCHAR(256),
                                      'news_time':VARCHAR(64),
                                      'href':VARCHAR(256),
                                      'content':NVARCHAR(),
                                      'update_datetime':DATETIME,
                                      'news_source':VARCHAR(32)})
            self.session.execute(SQL_MERGE_MACRO_NEWS)
            self.session.commit()
        self.last_flag = None
     
class SpiderMaster:
    def __init__(self):
        self.spider_nests = []
        
    def run(self):
        for spider in self.spider_nests:
            spider.init_spider()
            
        for spider in self.spider_nests:
            spider.start()
            
        for spider in self.spider_nests:
            spider.join()
            
    def add_spider(self,spider):
        self.spider_nests.append(spider)
        
        