# -*- coding: utf-8 -*-
"""
Created on Thu Jul 19 09:15:33 2018

@author: ldh
"""

# macro1.py

import re
import threading
import random
import datetime as dt
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd

from sqlalchemy.types import TEXT,VARCHAR,DATETIME
from database_engine import DatabaseEngine
from SQLs import SQL_MERGE_MACRO_NEWS

retry_seconds = 600
update_seconds = 600
lock = threading.Lock()

def ip_generator():
    ip = [str(random.randint(0,255)) for i in range(4)]
    ip = '.'.join(ip)
    return ip

def header_generator():
    header = {'User-Agent': 
    'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'}
    header['X-Forwarded-For'] = ip_generator()
    return header

def ths_news_time_convertor(ths_time):
    '''
    同花顺新闻时间转换成正常时间。
    '''
    year = dt.datetime.today().year
    month = ths_time[:2]
    day = ths_time[3:5]
    hour = ths_time[7:9]
    minute = ths_time[10:12]
    return str(year) + '-' + month + '-' + day + ' ' + \
            hour + ':' + minute

def cnfol_news_time_convertor(cnfol_time):
    '''
    中金在线时间转换。
    '''
    year = dt.datetime.today().year
    return str(year) + '-' + cnfol_time[1:-1]


def collect_data_from_ths():
    engine_obj = DatabaseEngine('xiaoyi')
    engine = engine_obj.get_engine()
    session = engine_obj.get_session()
    url_ths = 'http://news.10jqka.com.cn/cjzx_list/index_1.shtml'
    last_news_time = None
    
    print 'THS collecter starts RUNNING...'
    while True:
        
        # 标题页数据获取
        response = requests.get(url_ths,headers = header_generator())
        
        if response.status_code == 200:
            pass
        else:
            time.sleep(retry_seconds + random.randint(0,30))
            continue
        
        # 标题页数据解析
        soup = BeautifulSoup(response.content,'html.parser')
        
        content = soup.find_all(class_ = 'content-1200')
        content = content[1]
        content = content.find_all(class_ = 'module-l fl')
        content = content[0]
        content = content.find_all(class_ = 'list-con')
        content = content[0]
        content_titles = content.find_all(class_ = 'arc-title')
        
        
        titles = []
        news_time = []
        hrefs = []

        
        for each in content_titles:
            tmp = each.find('a')
            titles.append(tmp.attrs['title'])
            news_time.append(each.find('span').text)
            hrefs.append(tmp.attrs['href'])
        
        news_time_adj = map(ths_news_time_convertor,news_time)
        
        # 判断是否有新的新闻写入
        news_df = pd.DataFrame([titles,news_time_adj,hrefs],
                              index = ['title','news_time','href']).T
        news_df['news_source'] = u'同花顺'
        if last_news_time is None:
            last_news_time = max(news_time_adj)
            additions = news_df
        else:
            latest_news_time = max(news_time_adj)
            if latest_news_time <= last_news_time:
                time.sleep(update_seconds + random.randint(0,30))
                continue # 无更新内容
            else:
                # 确定增量更新内容
                additions = news_df.loc[news_df['news_time'] > last_news_time]
                additions['content'] = None
                last_news_time = latest_news_time
                
        # 增量爬取
        for idx,row in additions.iterrows():
            link = row['href']
            link_response = requests.get(link,headers = header_generator())
            if link_response.status_code == 404:
                continue
            tmp_soup = BeautifulSoup(link_response.content,'html.parser')
            main_text = tmp_soup.find_all(class_ = 'main-text atc-content')
            main_text = main_text[0]
            main_text_list = main_text.find_all('p')
            main_text_content = [each.text for each in main_text_list][1:-1]
            tmp_content = '\n'.join(main_text_content)
            last_stop_point = tmp_content.rfind(u'。')
            filtered_content = tmp_content[:last_stop_point + 1]
            additions.loc[idx,'content'] = filtered_content
        
    
        
        # 爬取内容增量写入数据库
        additions['update_datetime'] = dt.datetime.today()
        lock.acquire() # 防止同一时间有其他进程对数据库进行写入
        additions.to_sql('news_macro_source',engine,index = False,if_exists = 'replace',
                         dtype = {'title':VARCHAR(256),
                                  'news_time':VARCHAR(64),
                                  'href':VARCHAR(256),
                                  'content':TEXT,
                                  'update_datetime':DATETIME,
                                  'news_source':VARCHAR(32)})
        session.execute(SQL_MERGE_MACRO_NEWS)
        session.commit()
        lock.release()
        
        # 休眠
        time.sleep(update_seconds + random.randint(0,30))

def collect_data_from_cnfol():
    '''
    中金在线数据爬取.
    '''
    engine_obj = DatabaseEngine('xiaoyi')
    engine = engine_obj.get_engine()
    session = engine_obj.get_session()
    
    url_cnfol = 'http://news.cnfol.com/guoneicaijing/'
    last_news_time = None
    print 'CNFOL collecter starts RUNNING...'
    
    while True:
        
        # 获取标题页
        response = requests.get(url_cnfol,headers = header_generator())
        if response.status_code == 200:
            pass
        else:
            time.sleep(retry_seconds + random.randint(0,30))
            continue
        soup = BeautifulSoup(response.content,'html.parser')
        
        content = soup.find('ul',class_ = 'TList')
        
        # 标题页解析
        titles = []
        hrefs = []
        news_time = []
        
        content_list = content.find_all('li')
        
        for tag in content_list:
            tag1 = tag.find('a')
            
            titles.append(tag1.attrs['title'])
            hrefs.append(tag1.attrs['href'])
            news_time.append(cnfol_news_time_convertor(tag.find('span').text))
            
        news_df = pd.DataFrame([titles,news_time,hrefs],index = ['title','news_time',
                               'href']).T
        news_df['news_source'] = u'中金在线'
        
        # 判断是否有新的新闻写入
        if last_news_time is None:
            last_news_time = max(news_time)
            additions = news_df
        else:
            latest_news_time = max(news_time)
            if latest_news_time <= last_news_time:
                time.sleep(update_seconds + random.randint(0,30))
                continue # 无更新内容
            else:
                # 确定增量更新内容
                additions = news_df.loc[news_df['news_time'] > last_news_time]
                additions['content'] = None
                last_news_time = latest_news_time
                
        # 增量爬取
        for idx,row in additions.iterrows():
            link = row['href']
            link_response = requests.get(link,headers = header_generator())
            if link_response.status_code == 404:                
                continue
            tmp_soup = BeautifulSoup(link_response.content,'html.parser')
            main_text = tmp_soup.find('div',class_ = 'Article')
            filtered_content = main_text.text
            additions.loc[idx,'content'] = filtered_content 
              
        # 爬取内容增量写入数据库
        additions['update_datetime'] = dt.datetime.today()
        lock.acquire() # 防止同一时间有其他进程对数据库进行写入
        additions.to_sql('news_macro_source',engine,index = False,if_exists = 'replace',
                         dtype = {'title':VARCHAR(256),
                                  'news_time':VARCHAR(64),
                                  'href':VARCHAR(256),
                                  'content':TEXT,
                                  'update_datetime':DATETIME,
                                  'news_source':VARCHAR(32)})
        session.execute(SQL_MERGE_MACRO_NEWS)
        session.commit()
        lock.release()
        
        # 休眠
        time.sleep(update_seconds + random.randint(0,30))        

def collect_data_from_jrj():
    '''
    金融界数据爬取.
    '''
    engine_obj = DatabaseEngine('xiaoyi')
    engine = engine_obj.get_engine()
    session = engine_obj.get_session()
    
    url_jrj = 'http://finance.jrj.com.cn/list/guoneicj.shtml'
    last_news_title = None
    print 'JRJ collecter starts RUNNING...'
    
    while True:
        
        # 获取标题页
        response = requests.get(url_jrj,headers = header_generator())
        if response.status_code == 200:
            pass
        else:
            time.sleep(retry_seconds + random.randint(0,30))
            continue
        soup = BeautifulSoup(response.content,'html.parser')
        
        content = soup.find('ul',class_ = 'list2')
        
        # 标题页解析
        titles = []
        hrefs = []
        
        content_list = content.find_all('li')
        
        for tag in content_list:
            if 'class' in tag.attrs.keys():
                continue
            tag1 = tag.find('a')
            
            titles.append(tag1.text)
            hrefs.append(tag1.attrs['href'])
            
        news_df = pd.DataFrame([titles,hrefs],index = ['title',
                               'href']).T
        news_df['news_source'] = u'金融界'
        
        # 判断是否有新的新闻写入(根据标题)
        if last_news_title is None:
            additions = news_df
        else:
            title_list = news_df['title'].tolist()
            try:
                last_title_idx = title_list.index(last_news_title)
                if last_title_idx == 0:
                    time.sleep(update_seconds + random.randint(0,30))   
                    continue
                additions = news_df.loc[:last_title_idx]
            except:
                additions = news_df
        last_news_title = additions['title'].iloc[0]
        additions['content'] = None
        additions['news_time'] = None
        
        # 增量爬取
        for idx,row in additions.iterrows():
            link = row['href']
            link_response = requests.get(link,headers = header_generator())
            if link_response.status_code == 404:                
                continue
            tmp_soup = BeautifulSoup(link_response.content,'html.parser')
            
            # 获取文本
            main_text = tmp_soup.find('div',class_ = 'texttit_m1')
            filtered_content = main_text.text
            last_stop_point = filtered_content.rfind(u'。')
            filtered_content = filtered_content[:last_stop_point + 1]
            additions.loc[idx,'content'] = filtered_content 
            
            # 获取时间
            news_time = tmp_soup.find(text = re.compile(r'\d{4}-\d{2}-\d{2}'))[2:]
            additions.loc[idx,'news_time'] = news_time[:-3]
              
        # 爬取内容增量写入数据库
        additions['update_datetime'] = dt.datetime.today()

        lock.acquire() # 防止同一时间有其他进程对数据库进行写入
        additions.to_sql('news_macro_source',engine,index = False,if_exists = 'replace',
                         dtype = {'title':VARCHAR(256),
                                  'news_time':VARCHAR(64),
                                  'href':VARCHAR(256),
                                  'content':TEXT,
                                  'update_datetime':DATETIME,
                                  'news_source':VARCHAR(32)})
        session.execute(SQL_MERGE_MACRO_NEWS)
        session.commit()
        lock.release()
        # 休眠
        time.sleep(update_seconds + random.randint(0,30))        
            
        
        

if __name__ == '__main__':
    print 'Start to collect data from the website'
    print 'Please DONOT turn this programme off'
    
    t_ths = threading.Thread(target = collect_data_from_ths, name = 'THS')
    t_cnfol = threading.Thread(target = collect_data_from_cnfol, name = 'CNFOL')
    t_jrj = threading.Thread(target = collect_data_from_jrj, name = 'JRJ')
    t_ths.start()
    t_cnfol.start()
    t_jrj.start()
    t_ths.join()    
    t_cnfol.join()
    t_jrj.join()