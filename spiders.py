# -*- coding: utf-8 -*-
"""
Created on Fri Jul 20 16:40:02 2018

@author: ldh
"""

# spiders.py

import datetime as dt
import urlparse
import requests
from bs4 import BeautifulSoup
import pandas as pd

from spider_base import NewsSpider
from utils import ths_news_time_convertor

class THSSpider(NewsSpider):
    def __init__(self,lock,update_seconds = 600):
        super(THSSpider,self).__init__('THS',u'同花顺',
             'http://news.10jqka.com.cn/cjzx_list/index_1.shtml',
             lock,update_seconds)
        
    def _parse_titles_response(self):
        soup = BeautifulSoup(self.titles_response.content,'html.parser')
        content_titles = soup.find_all('span',class_ = 'arc-title')
                
        titles = []
        news_time = []
        hrefs = []
        
        for each in content_titles:
            tmp = each.find('a')
            titles.append(tmp.attrs['title'])
            news_time.append(ths_news_time_convertor(each.find('span').text))
            hrefs.append(tmp.attrs['href'])

        news_df = pd.DataFrame([titles,news_time,hrefs],
                              index = ['title','news_time','href']).T         
        
        # 判断是否有新的新闻写入        
        if self.last_flag is None:
            self.last_flag = max(news_time)
            self.additions = news_df
            self.additions.loc[:,'content'] = None
        else:
            latest_news_time = max(news_time)
            if latest_news_time <= self.last_flag:
                return # 无更新内容
            else:
                # 确定增量更新内容
                self.additions = news_df.loc[news_df['news_time'] > self.last_flag]
                self.additions.loc[:,'content'] = None
                self.last_flag = latest_news_time
                
        self.additions.loc[:,'news_source'] = self.source_name
        
    def _parse_content_response(self,idx):
        tmp_soup = BeautifulSoup(self.content_response.content,'html.parser')
        try:
            main_text = tmp_soup.find_all(class_ = 'main-text atc-content')
            main_text = main_text[0]
            main_text_list = main_text.find_all('p')
            main_text_content = [each.text for each in main_text_list][1:-1]
            tmp_content = '\n'.join(main_text_content)
            last_stop_point = tmp_content.rfind(u'。')
            filtered_content = tmp_content[:last_stop_point + 1]
            self.additions.loc[idx,'content'] = filtered_content
        except:
            self.additions.loc[idx,'content'] = None
                
        self.additions.loc[:,'update_datetime'] = dt.datetime.today()  
        return tmp_soup
    
class ZZWSpdier(NewsSpider):
    def __init__(self,lock,update_seconds = 600):
        super(ZZWSpdier,self).__init__('ZZW',u'中证网',
             'http://www.cs.com.cn/xwzx/',
             lock,update_seconds)
        
    def _parse_titles_response(self):
        content = self.titles_response.content
        soup = BeautifulSoup(content,'html.parser')
        
        content_titles = soup.find('ul',class_ = 'list-lm')
        content_titles = content_titles.find_all('li')
        news_time = [each.find('span').text for each in content_titles]
        hrefs = [each.find('a').attrs['href'] for each in content_titles]
        titles = [each.find('a').text for each in content_titles]
        
        news_time = map(lambda x: '20' + x,news_time)
        hrefs = map(lambda x: urlparse.urljoin(self.title_url,x),hrefs)

        news_df = pd.DataFrame([titles,news_time,hrefs],index = ['title','news_time','href']).T
        
        # 判断是否有新的新闻写入        
        if self.last_flag is None:
            self.last_flag = max(news_time)
            self.additions = news_df
            self.additions.loc[:,'content'] = None
        else:
            latest_news_time = max(news_time)
            if latest_news_time <= self.last_flag:
                return # 无更新内容
            else:
                # 确定增量更新内容
                self.additions = news_df.loc[news_df['news_time'] > self.last_flag]
                self.additions.loc[:,'content'] = None
                self.last_flag = latest_news_time  
                
        self.additions.loc[:,'news_source'] = self.source_name               
    
    def _parse_content_response(self,idx):
        encoding = requests.utils.get_encodings_from_content(self.content_response.text)
        self.content_response.encoding = encoding[0]
        content = self.content_response.text
        try:
            tmp_soup = BeautifulSoup(content,'html.parser')
            article = tmp_soup.find('div',class_ = 'article-t hidden')
            article = article.find_all('p')
            article = [each.text for each in article]
            article = '\n'.join(article)
            self.additions.loc[idx,'content'] = article
        except:
            self.additions.loc[idx,'content'] = None
        self.additions.loc[:,'update_datetime'] = dt.datetime.today()  
        
    
class CNSTOCKSpider(NewsSpider):
    def __init__(self,lock,update_seconds = 600):
        super(CNSTOCKSpider,self).__init__('CNSTOCK',u'中国证券网',
             'http://news.cnstock.com/news/sns_yw/index.html',
             lock,update_seconds)  
        
    def _parse_titles_response(self):
        content = self.titles_response.content
        soup = BeautifulSoup(content,'html.parser')
        content_titles = soup.find('ul',class_ = 'new-list article-mini')
        news_time = [each.text[1:-1] for each in content_titles.find_all('span',class_ = 'time')]
        hrefs = [each.attrs['href'] for each in content_titles.find_all('a')]
        titles = [each.attrs['title'] for each in content_titles.find_all('a')]
        
        news_df = pd.DataFrame([titles,news_time,hrefs],index = ['title',
                               'news_time','href']).T
            
        if self.last_flag is None:
            self.last_flag = max(news_time)
            self.additions = news_df
            self.additions.loc[:,'content'] = None
        else:
            latest_news_time = max(news_time)
            if latest_news_time <= self.last_flag:
                return # 无更新内容
            else:
                # 确定增量更新内容
                self.additions = news_df.loc[news_df['news_time'] > self.last_flag]
                self.additions.loc[:,'content'] = None
                self.last_flag = latest_news_time  
        self.additions.loc[:,'news_source'] = self.source_name          
        
    def _parse_content_response(self,idx):
        tmp_soup = BeautifulSoup(self.content_response.content,'html.parser')
        
        content = tmp_soup.find('div',class_ = 'content')
        content_list = content.find_all('p')
        content_list = [each.text for each in content_list]
        content = '\n'.join(content_list)
        self.additions.loc[idx,'content'] = content
        self.additions.loc[:,'update_datetime'] = dt.datetime.today()  
        
class STCNSpider(NewsSpider):
    def __init__(self,lock,update_seconds = 600,news_limit = 10):
        super(STCNSpider,self).__init__('STCN',u'证券时报网',
             'http://news.stcn.com/',
             lock,update_seconds) 
        self.news_limit = news_limit
        
    def _parse_titles_response(self):
        content = self.titles_response.content
        soup = BeautifulSoup(content,'html.parser')
        content_titles = soup.find_all(class_ = 'tit')[:self.news_limit]
        news_time = soup.find_all(class_ = 'sj')[:self.news_limit]
        
        titles = [each.find('a').attrs['title'] for each in content_titles]
        hrefs = [each.find('a').attrs['href'] for each in content_titles]
        news_time = [each.text[:10] + ' ' + each.text[10:] for each in news_time]
        
        news_df = pd.DataFrame([titles,news_time,hrefs],index = ['title',
            'news_time','href']).T
    
    
        if self.last_flag is None:
            self.last_flag = max(news_time)
            self.additions = news_df
            self.additions.loc[:,'content'] = None
        else:
            latest_news_time = max(news_time)
            if latest_news_time <= self.last_flag:
                return # 无更新内容
            else:
                # 确定增量更新内容
                self.additions = news_df.loc[news_df['news_time'] > self.last_flag]
                self.additions.loc[:,'content'] = None
                self.last_flag = latest_news_time  
        self.additions.loc[:,'news_source'] = self.source_name          
       
 
    def _parse_content_response(self,idx):
        tmp_soup = BeautifulSoup(self.content_response.content,'html.parser')
        content = tmp_soup.find(class_ = 'txt_con')
        content_list = content.find_all('p')
        content_list = [each.text for each in content_list]
        content = '\n'.join(content_list)
        
        self.additions.loc[idx,'content'] = content
        self.additions.loc[:,'update_datetime'] = dt.datetime.today()      
        
class PeopleSpider(NewsSpider):
    def __init__(self,lock,update_seconds = 600):
        super(PeopleSpider,self).__init__('PEOPLE',u'人民网',
             'http://finance.people.com.cn/index1.html',
             lock,update_seconds)
        
    def _parse_titles_response(self):

        self.titles_response.encoding = self.titles_response.apparent_encoding
        content = self.titles_response.text
        
        soup = BeautifulSoup(content,'html.parser')
        related = soup.find_all('div',class_ = 'left w310')
        
        for each in related:
            if each.find('h2'):
                if each.find('h2').text == u'宏观':
                    hrefs = each
                    break
                
        hrefs = [each.attrs['href'] for each in hrefs.find_all('a')]
        hrefs = [urlparse.urljoin(self.title_url,each) for each in hrefs]
                
        news_df = pd.DataFrame([hrefs],index = ['href']).T
        
        if self.last_flag is None:
            self.last_flag = max(hrefs) # flag是href
            self.additions = news_df
            self.additions.loc[:,'content'] = None
        else:
            latest_href = max(hrefs)
            if latest_href <= self.last_flag:
                return # 无更新内容
            else:
                # 确定增量更新内容
                self.additions = news_df.loc[news_df['href'] > self.last_flag]
                self.additions.loc[:,'content'] = None
                self.last_flag = latest_href
                
        self.additions.loc[:,'news_source'] = self.source_name          

    
    def _parse_content_response(self,idx):
        
        self.content_response.encoding = self.content_response.apparent_encoding
        tmp_soup = BeautifulSoup(self.content_response.text,'html.parser')
        
        header = tmp_soup.find('div',class_ = 'clearfix w1000_320 text_title')
        title = header.find('h1').text
        news_time = header.find(class_ = 'fl').text[:16]
        news_time = news_time.replace(u'年','-')
        news_time = news_time.replace(u'月','-')
        news_time = news_time.replace(u'日',' ') 
        
        content = tmp_soup.find(class_ = 'box_con')
        content_list = ['  ' + each.text.strip() for each in content.find_all('p')]
        content = '\n'.join(content_list)
        
        self.additions.loc[idx,'title'] = title
        self.additions.loc[idx,'news_time'] = news_time
        self.additions.loc[idx,'content'] = content
        self.additions.loc[:,'update_datetime'] = dt.datetime.today()   
        
if __name__ == '__main__':
#    ths_spider = THSSpider(None)
#    ths_spider.test_spider(True)
    from threading import Lock
    lock = Lock()
#    zzw_spider = ZZWSpdier(lock)
#    zzw_spider.init_spider()
#    zzw_spider.spider_data()
    cnstock_spider = CNSTOCKSpider(lock)
    cnstock_spider.init_spider()
    cnstock_spider.start()
    cnstock_spider.join()
#    stcn_spider = STCNSpider(None)
#    stcn_spider.test_spider()    
#    people_spider = PeopleSpider(None)
#    people_spider.test_spider()