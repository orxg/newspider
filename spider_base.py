# -*- coding: utf-8 -*-
"""
Created on Fri Jul 20 14:57:43 2018

@author: ldh
"""

# spider_base.py

import time
import datetime as dt
import random
import threading
from sqlalchemy.types import VARCHAR,DATETIME,NVARCHAR,INT
import pymssql
import requests
import pandas as pd

from database_engine import DatabaseEngine
from SQLs import (SQL_MERGE_MACRO_NEWS,SQL_MERGE_MACRO_NEWS_FOR_HEADER,
                  SQL_MERGE_MACRO_NEWS_FOR_ALL,
                  SQL_GET_MACRO_NEWS_TOP,
                  SQL_MERGE_MACRO_NEWS_REPEAT_TIMES)
from utils import header_generator
from consts import SPIDER_LIVE_TIME

class NewsSpider(object):
    def __init__(self,spider_name,source_name,title_url,lock,
                 update_seconds = 30):
        '''
        新闻爬虫工厂。
        生产流程:
            1. 实现parse_title生成self.additions
            2. 实现parse_content,将相关内容添加到self.additions.  
        
        Parameters
        ----------
        spider_name
            爬虫名称,英文
        source_name
            新闻源名称,unicode
        title_url
            标题页url
        lock
            线程共享锁
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
        spider_birth_time = time.time()
        while time.time() - spider_birth_time < SPIDER_LIVE_TIME:
            # 获取标题页
            try:
                status_code = self._get_titles_response()
                
                if status_code != 200:
                    continue
                
                # ----------- 标题页分析,确定增量更新内容self.additions -----------
                # additions.columns = [title,href,news_time,content,news_source]
                # 保证additions都有上述的column,没有内容的用None
                try:
                    self._parse_titles_response()
                except Exception as e:
                        print 'Spider %s failed to parse TITLE'%self.spider_name
                        print 'Error:'
                        print e
                        print '*' * 40
                        self._have_a_short_rest()
                        continue
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
                    try:
                        self._parse_content_response(idx)
                    except Exception as e:
                        print 'Spider %s failed to parse content'%self.spider_name
                        print 'The href is %s'%href
                        print 'Error:'
                        print e
                        print '*' * 40
                    
                
                self._add_unique_flag()
                self._check_additions()
                
                if len(self.additions) == 0:
                    self._have_a_rest()
                    continue
                
                self._add_header_flag()            
    
                self._write_into_db()
                self._have_a_rest()
            except Exception as e:
                print '[%s]Error in %s'%(dt.datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
                       self.spider_name)            
                print 'Error: %s'%e
                break
        

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
        self.additions = self.additions.drop_duplicates(subset = 'unique_flag')
        
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
                           u'？', u'。',u'，',u'！',u'：',u'”',u'“',u'、',u'《',u'\u3000'
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
        # ---------------------
        # 1.
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
        try:
            if not self.if_header:
                self.session.execute(SQL_MERGE_MACRO_NEWS)
                self.session.commit()
            else:
                self.session.execute(SQL_MERGE_MACRO_NEWS_FOR_HEADER)
                self.session.commit()
                
            self.session.execute(SQL_MERGE_MACRO_NEWS_FOR_ALL)
            self.session.commit()
        except pymssql.OperationalError:
            print 'Error of %s: Writing into database failed'%self.spider_name
            
        # ---------------------
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
        self.news_counter = NewsCounter()
        
    def run(self):
        self.news_counter.start()
        
        for spider in self.spider_nests:
            spider.init_spider()
            
        for spider in self.spider_nests:
            spider.start()
            
        for spider in self.spider_nests:
            spider.join()
        self.news_counter.join()
        
    def add_spider(self,spider):
        self.spider_nests.append(spider)
        
class NewsCounter:
    def __init__(self,sleep_time = 60):
        self.counter_thread = threading.Thread(target = self.run,
                                               name = 'Counter')
        self.recent_data = None
        self.sleep_time = sleep_time
        
        self._init_counter()
        
    def _init_counter(self):
        self.engine_obj = DatabaseEngine('xiaoyi')
        self.engine = self.engine_obj.get_engine()
        self.session = self.engine_obj.get_session()
        
    def start(self):
        self.counter_thread.start()
        
    def join(self):
        self.counter_thread.join()
        
    def run(self):
        print 'News Counter starts to run...'
        counter_birth_time = time.time()
        while time.time() - counter_birth_time < SPIDER_LIVE_TIME:
            self.recent_data = pd.read_sql(SQL_GET_MACRO_NEWS_TOP,self.engine)
            self.recent_data_group = self.recent_data.groupby('unique_flag',
                                                         sort = False)
            self.news_repeat = self.recent_data_group.count()
            self.news_repeat = self.news_repeat.reset_index()
            self.news_repeat.rename(columns = {'news_source':'repeat_times'},
                                    inplace = True)
            self.news_repeat.drop_duplicates(subset = ['unique_flag'],
                                             inplace = True)
            # write
            self.news_repeat.to_sql('news_repeat',self.engine,index = False,
                                    if_exists = 'replace',
                                    dtype = {'unique_flag':VARCHAR(256),
                                             'repeat_times':INT})
            self.session.execute(SQL_MERGE_MACRO_NEWS_REPEAT_TIMES)
            self.session.commit()            
            time.sleep(self.sleep_time)
        
    
class HeaderCollector:
    def __init__(self):
        self.collector_thread = threading.Thread()
        self.news_num = 12
        
    def run(self):
        # 读取最新头条
        pass
    
    
    
        