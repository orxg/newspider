# -*- coding: utf-8 -*-
"""
Created on Fri Jul 20 16:40:02 2018

@author: ldh
"""

# spiders.py

import re
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
                self.additions = pd.DataFrame()
                return # 无更新内容
            else:
                # 确定增量更新内容
                self.additions = news_df.loc[news_df['news_time'] > self.last_flag]
                self.additions.loc[:,'content'] = None
                self.last_flag = latest_news_time
                
        self.additions.loc[:,'news_source'] = self.source_name
        
    def _parse_content_response(self,idx):
        tmp_soup = BeautifulSoup(self.content_response.content,'html.parser')
#        try:
        main_text = tmp_soup.find(class_ = 'main-text atc-content')
        bottom = main_text.find(class_ = 'bottomSign')
        bottom.clear()
        js_script = main_text.find(type = 'text/javascript')
        js_script.clear()
        
        # 清除非图片链接
        for a in main_text.find_all():
            if a.has_attr('href'):
                del a['href']
                
        news_content = str(main_text).decode('utf8').replace(u'　',u' ')
                
        self.additions.loc[idx,'content'] = news_content
                
        self.additions.loc[:,'update_datetime'] = dt.datetime.today()
#        except:
#            self.additions.loc[idx,'content'] = None                    
#            self.additions.loc[:,'update_datetime'] = dt.datetime.today()            
        
    
class ZZWSpdier(NewsSpider):
    def __init__(self,lock,update_seconds = 600):
        super(ZZWSpdier,self).__init__('ZZW',u'中证网',
             'http://www.cs.com.cn/xwzx/hg/',
             lock,update_seconds)
        
    def _parse_titles_response(self):
        content = self.titles_response.content
        soup = BeautifulSoup(content,'html.parser')
        
        content_titles = soup.find('ul',class_ = 'list-lm pad10')
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
                self.additions = pd.DataFrame()
                return # 无更新内容
            else:
                # 确定增量更新内容
                self.additions = news_df.loc[news_df['news_time'] > self.last_flag]
                self.additions.loc[:,'content'] = None
                self.last_flag = latest_news_time  
                
        self.additions.loc[:,'news_source'] = self.source_name               
    
    def _parse_content_response(self,idx):
        try:
            encoding = requests.utils.get_encodings_from_content(self.content_response.text)
            self.content_response.encoding = encoding[0]
            
            # parse content
            content = self.content_response.text
            tmp_soup = BeautifulSoup(content,'html.parser')
            article = tmp_soup.find('div',class_ = 'article-t hidden')
            
            # 解析总页数
            js_script = article.find(class_ = 'page').text
            count_page_str = re.findall(r'var countPage = (\d+)',js_script)
            count_page = int(count_page_str[0])
            
            # 剔除javascript代码部分
            pages_js = article.find(class_ = 'page')
            pages_js.clear() 
            
            if count_page == 1:        
                self.additions.loc[idx,'content'] = str(article).decode('utf8').replace(u'　',u' ')
            else: # 爬取其他页面
                link_compoent_list = self.content_response.url.split('.')
                last_file = link_compoent_list[-2]
                pages_list = [last_file + '_' + str(i) for i in range(1,count_page)]
                
                other_links = []
                for i in range(0,count_page - 1):
                    link_compoent_list[-2] = pages_list[i]
                    other_links.append('.'.join(link_compoent_list))
                
                content_all = str(article).decode('utf8')
                
                for other_link in other_links:
                    link_response = requests.get(other_link,headers = self.header_generator())            
                    encoding = requests.utils.get_encodings_from_content(link_response.text)
                    link_response.encoding = encoding[0]    
                    content = link_response.text
                    tmp_soup = BeautifulSoup(content,'html.parser')
                    article = tmp_soup.find('div',class_ = 'article-t hidden')
                    pages_js = article.find(class_ = 'page')
                    pages_js.clear() 
                    content_all += str(article).decode('utf8').replace(u'　',u' ')
                    
                self.additions.loc[idx,'content'] = content_all
        except:
            self.additions.loc[idx,'content'] = None
        self.additions.loc[:,'update_datetime'] = dt.datetime.today()  
        
    
class CNSTOCKSpider(NewsSpider):
    def __init__(self,lock,update_seconds = 600):
        super(CNSTOCKSpider,self).__init__('CNSTOCK',u'中国证券网',
             'http://news.cnstock.com/news/sns_yw/index.html',
             lock,update_seconds)  
        self.web_url = 'http://www.cnstock.com'
        
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
                self.additions = pd.DataFrame()
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
        if content is not None:
            imgs = content.find_all('img')
            if len(imgs) != 0:
                for img in imgs:
                    img.attrs['src'] = urlparse.urljoin(self.web_url,img.attrs['src'])
            content = str(content).decode('utf8').replace(u'　',u' ')
        else:
            content = None
        
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
        
        titles = [each.find('a').text for each in content_titles]
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
                self.additions = pd.DataFrame()
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
        content = str(content).decode('utf8').replace(u'　',u' ')
        
        self.additions.loc[idx,'content'] = content
        self.additions.loc[:,'update_datetime'] = dt.datetime.today()      
        
class PeopleSpider(NewsSpider):
    def __init__(self,lock,update_seconds = 600):
        super(PeopleSpider,self).__init__('PEOPLE',u'人民网',
             'http://finance.people.com.cn/index1.html',
             lock,update_seconds)
        self.web_url = 'http://finance.people.com.cn/'
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
                self.additions = pd.DataFrame()
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
        title = header.find('h1').text.replace(u'\xa0',u' ')
        news_time = header.find(class_ = 'fl').text[:16]
        news_time = news_time.replace(u'年','-')
        news_time = news_time.replace(u'月','-')
        news_time = news_time.replace(u'日',' ') 
        
        content = tmp_soup.find(class_ = 'box_con')

        pics = content.find_all('img')
        for pic in pics:
            pic['src'] = urlparse.urljoin(self.web_url,pic['src'])    
        content = str(content).decode('utf8').replace(u'　',u' ').replace(u'\xa0',u' ')
        
        self.additions.loc[idx,'title'] = title
        self.additions.loc[idx,'news_time'] = news_time
        self.additions.loc[idx,'content'] = content
        self.additions.loc[:,'update_datetime'] = dt.datetime.today() 

class HeaderPeopleSpider(NewsSpider):
    def __init__(self,lock,update_seconds = 600):
        super(HeaderPeopleSpider,self).__init__('HEADER_PEOPLE',u'人民网',
             'http://finance.people.com.cn/',
             lock,update_seconds)
        self.if_header = 1
        
    def _parse_titles_response(self):

        self.titles_response.encoding = self.titles_response.apparent_encoding
        content = self.titles_response.text
        
        soup = BeautifulSoup(content,'html.parser')
        related = soup.find('div',class_ = 'title mt15')
        related1 = related.find('a')
        
        href = urlparse.urljoin(self.title_url,related1.attrs['href'])
        title = related1.text.replace(u'\xa0',u' ')
                
        news_df = pd.DataFrame([[title,href]],columns = ['title','href'])
        
        self.additions = news_df

        if self.last_flag is None:
            self.last_flag = href # flag是href
            self.additions = news_df
            self.additions.loc[:,'content'] = None
        else:
            latest_href = href
            if latest_href <= self.last_flag:
                self.additions = pd.DataFrame()
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
        news_time = header.find(class_ = 'fl').text[:16]
        news_time = news_time.replace(u'年','-')
        news_time = news_time.replace(u'月','-')
        news_time = news_time.replace(u'日',' ') 
        
        content = tmp_soup.find(class_ = 'box_con')

        pics = content.find_all('img')
        for pic in pics:
            pic['src'] = urlparse.urljoin(self.title_url,pic['src'])    
        content = str(content).decode('utf8').replace(u'　',u' ').replace(u'\xa0',u' ')
        
        self.additions.loc[idx,'news_time'] = news_time
        self.additions.loc[idx,'content'] = content
        self.additions.loc[:,'update_datetime'] = dt.datetime.today()

class HeaderSTCNSpider(NewsSpider):
    def __init__(self,lock,update_seconds = 600):
        super(HeaderSTCNSpider,self).__init__('HEADER_STCN',u'证券时报网',
             'http://news.stcn.com/',
             lock,update_seconds) 
        self.if_header = 1
        
    def _parse_titles_response(self):
        content = self.titles_response.content
        soup = BeautifulSoup(content,'html.parser')
        hot_news = soup.find(class_ = 'hotNews')
        
        href = hot_news.dt.a.attrs['href']
        title = hot_news.dd.text
        news_time = hot_news.find(class_ = 'sj')
        news_time = news_time.text[:10] + ' ' + news_time.span.text
        
        news_df = pd.DataFrame([[title,news_time,href]],columns = ['title',
            'news_time','href'])
        
        if self.last_flag is None:
            self.last_flag = news_time
            self.additions = news_df
            self.additions.loc[:,'content'] = None
        else:
            latest_news_time = news_time
            if latest_news_time <= self.last_flag:
                self.additions = pd.DataFrame()
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
        content = str(content).decode('utf8').replace(u'　',u' ')
        
        self.additions.loc[idx,'content'] = content
        self.additions.loc[:,'update_datetime'] = dt.datetime.today() 

class HeaderZZWSpdier(NewsSpider):
    def __init__(self,lock,update_seconds = 600):
        super(HeaderZZWSpdier,self).__init__('HEADER_ZZW',u'中证网',
             'http://www.cs.com.cn/xwzx/',
             lock,update_seconds)
        self.if_header = 1
        
    def _parse_titles_response(self):
        content = self.titles_response.content
        soup = BeautifulSoup(content,'html.parser')
        
        top_news = soup.find(class_ = 'topnews hidden')
        title = top_news.h1.text
        href = urlparse.urljoin(self.title_url,top_news.h1.a.attrs['href'])
        
        news_df = pd.DataFrame([[title,href]],columns = ['title','href'])
        
        # 判断是否有新的新闻写入        
        if self.last_flag is None:
            self.last_flag = href # href作为标记
            self.additions = news_df
            self.additions.loc[:,'content'] = None
        else:
            latest_href = href
            if latest_href <= self.last_flag:
                self.additions = pd.DataFrame()
                return # 无更新内容
            else:
                # 确定增量更新内容
                self.additions = news_df
                self.additions.loc[:,'content'] = None
                self.last_flag = latest_href 
                
        self.additions.loc[:,'news_time'] = None
        self.additions.loc[:,'news_source'] = self.source_name               
        
    def _parse_content_response(self,idx):
        try:
            encoding = requests.utils.get_encodings_from_content(self.content_response.text)
            self.content_response.encoding = encoding[0]
            
            # parse content
            content = self.content_response.text
            tmp_soup = BeautifulSoup(content,'html.parser')
            article = tmp_soup.find('div',class_ = 'article-t hidden')
            # 解析时间
            info = tmp_soup.find(class_ = 'info')
            news_time = info.find('em').text
            self.additions.loc[idx,'news_time'] = news_time
            
            # 解析总页数
            js_script = article.find(class_ = 'page').text
            count_page_str = re.findall(r'var countPage = (\d+)',js_script)
            count_page = int(count_page_str[0])
            
            # 剔除javascript代码部分
            pages_js = article.find(class_ = 'page')
            pages_js.clear() 
            
            if count_page == 1:        
                self.additions.loc[idx,'content'] = str(article).decode('utf8').replace(u'　',u' ')
            else: # 爬取其他页面
                link_compoent_list = self.content_response.url.split('.')
                last_file = link_compoent_list[-2]
                pages_list = [last_file + '_' + str(i) for i in range(1,count_page)]
                
                other_links = []
                for i in range(0,count_page - 1):
                    link_compoent_list[-2] = pages_list[i]
                    other_links.append('.'.join(link_compoent_list))
                
                content_all = str(article).decode('utf8')
                
                for other_link in other_links:
                    link_response = requests.get(other_link,headers = self.header_generator())            
                    encoding = requests.utils.get_encodings_from_content(link_response.text)
                    link_response.encoding = encoding[0]    
                    content = link_response.text
                    tmp_soup = BeautifulSoup(content,'html.parser')
                    article = tmp_soup.find('div',class_ = 'article-t hidden')
                    pages_js = article.find(class_ = 'page')
                    pages_js.clear() 
                    content_all += str(article).decode('utf8').replace(u'　',u' ')
                    
                self.additions.loc[idx,'content'] = content_all
                
        except:
            self.additions.loc[idx,'content'] = None
#            self.additions.loc[idx,'news_time'] = None
            
        self.additions.loc[:,'update_datetime'] = dt.datetime.today() 

class HeaderCNSTOCKSpider(NewsSpider):
    def __init__(self,lock,update_seconds = 600):
        super(HeaderCNSTOCKSpider,self).__init__('HEADER_CNSTOCK',u'中国证券网',
             'http://www.cnstock.com/',
             lock,update_seconds)  
        self.if_header = 1
        self.web_url = 'http://www.cnstock.com'
        
    def _parse_titles_response(self):
        content = self.titles_response.content
        soup = BeautifulSoup(content,'html.parser')
        
        header_topic = soup.find(class_ = 'hd-topic')
        href = header_topic.h1.a.attrs['href']
        title = header_topic.h1.a.text
        
        news_df = pd.DataFrame([[title,href]],columns = ['title','href'])
            
        if self.last_flag is None:
            self.last_flag = href
            self.additions = news_df
            self.additions.loc[:,'content'] = None
        else:
            latest_href = href
            if latest_href <= self.last_flag:
                self.additions = pd.DataFrame()
                return # 无更新内容
            else:
                # 确定增量更新内容
                self.additions = news_df
                self.additions.loc[:,'content'] = None
                self.last_flag = latest_href  
        self.additions.loc[:,'news_source'] = self.source_name          
        self.additions.loc[:,'news_time'] = None
        
    def _parse_content_response(self,idx):
        tmp_soup = BeautifulSoup(self.content_response.content,'html.parser')
        
        content = tmp_soup.find('div',class_ = 'content')
        if content is not None:
            imgs = content.find_all('img')
            if len(imgs) != 0:
                for img in imgs:
                    img.attrs['src'] = urlparse.urljoin(self.web_url,img.attrs['src'])
            content = str(content).decode('utf8').replace(u'　',u' ')
            news_time = tmp_soup.find(class_ = 'timer').text
        else:
            content = None
            news_time = None
        
        self.additions.loc[idx,'content'] = content
        self.additions.loc[idx,'news_time'] = news_time
        self.additions.loc[:,'update_datetime'] = dt.datetime.today()     
        
if __name__ == '__main__':
#    ths_spider = THSSpider(None)
#    ths_spider.test_spider(True)
#    from threading import Lock
#    lock = Lock()
#    zzw_spider = ZZWSpdier(lock)
#    zzw_spider.init_spider()
#    zzw_spider.spider_data()
#    cnstock_spider = CNSTOCKSpider(lock)
#    cnstock_spider.init_spider()
#    cnstock_spider.start()
#    cnstock_spider.join()
#    stcn_spider = STCNSpider(None)
#    stcn_spider.test_spider()    
#    people_spider = PeopleSpider(None)
#    people_spider.test_spider()
    header_people_spider = HeaderPeopleSpider(None)