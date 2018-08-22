# -*- coding: utf-8 -*-
"""
Created on Mon Jul 23 09:41:51 2018

@author: ldh
"""

# spider_test.py

import urlparse
import re
import random
import datetime as dt
import time
import requests
from bs4 import BeautifulSoup,Tag
import urlparse
import pandas as pd

def ip_generator():
    ip = [str(random.randint(0,255)) for i in range(4)]
    ip = '.'.join(ip)
    return ip

def header_generator():
    header = {'User-Agent': 
    'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'}
    header['X-Forwarded-For'] = ip_generator()
    return header
#%% 同花顺
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
            
url_ths = 'http://news.10jqka.com.cn/cjzx_list/index_1.shtml'
last_news_time = None

    
    # 标题页数据获取
response = requests.get(url_ths,headers = header_generator())


# 标题页数据解析
soup = BeautifulSoup(response.content,'html.parser')
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

additions = news_df       
# 增量爬取
for idx,row in additions.iterrows():
    link = row['href']
    link_response = requests.get(link,headers = header_generator())
    
    tmp_soup = BeautifulSoup(link_response.content,'html.parser')
    main_text = tmp_soup.find(class_ = 'main-text atc-content')
    
    bottom = main_text.find(class_ = 'bottomSign')
    bottom.clear()
    js_script = main_text.find(type = 'text/javascript')
    js_script.clear()
    
    for a in main_text.find_all():
        if a.has_attr('href'):
            del a['href']
    news_content = str(main_text).decode('utf8').replace(u'　',u' ')
    p_list = main_text.find_all('p')
    for p in p_list:
        if p.has_attr('class'):
            if p.attrs['class'] == 'bottomSign':
                break
        else:
            news_content += str(p).decode('utf8')
    

    additions.loc[idx,'content'] = news_content
#%% 中证网
zzw_url = 'http://www.cs.com.cn/xwzx/hg/'

response = requests.get(zzw_url,headers = header_generator())

# parse titles
content = response.content
soup = BeautifulSoup(content,'html.parser')

content_titles = soup.find('ul',class_ = 'list-lm pad10')
content_titles = content_titles.find_all('li')
news_time = [each.find('span').text for each in content_titles]
hrefs = [each.find('a').attrs['href'] for each in content_titles]
titles = [each.find('a').text for each in content_titles]

news_time = map(lambda x: '20' + x,news_time)
hrefs = map(lambda x: urlparse.urljoin(zzw_url,x),hrefs)

latest_flag = max(news_time)
news_df = pd.DataFrame([titles,news_time,hrefs],index = ['title','news_time','href']).T

additions = news_df
additions['content'] = None
additions['news_source'] = u'中证网'

    
for idx,row in additions.iterrows():

    link = row['href']
    
    link_response = requests.get(link,headers = header_generator())
    
    encoding = requests.utils.get_encodings_from_content(link_response.text)
    link_response.encoding = encoding[0]
    
    # parse content
    content = link_response.text
    tmp_soup = BeautifulSoup(content,'html.parser')
    article = tmp_soup.find('div',class_ = 'article-t hidden')
    
    # 解析总页数
    js_script = article.find(class_ = 'page').text
    count_page_str = re.findall(r'var countPage = (\d+)',js_script)
    count_page = int(count_page_str[0])
    
    # 剔除javascript
    pages_js = article.find(class_ = 'page')
    pages_js.clear() 
    
    if count_page == 1:        
        additions.loc[idx,'content'] = str(article).decode('utf8')
    else:
        link_compoent_list = link.split('.')
        last_file = link_compoent_list[-2]
        pages_list = [last_file + '_' + str(i) for i in range(1,count_page)]
        
        other_links = []
        for i in range(0,count_page - 1):
            link_compoent_list[-2] = pages_list[i]
            other_links.append('.'.join(link_compoent_list))
        
        content_all = str(article).decode('utf8')
        
        for other_link in other_links:
            link_response = requests.get(other_link,headers = header_generator())            
            encoding = requests.utils.get_encodings_from_content(link_response.text)
            link_response.encoding = encoding[0]    
            content = link_response.text
            tmp_soup = BeautifulSoup(content,'html.parser')
            article = tmp_soup.find('div',class_ = 'article-t hidden')
            pages_js = article.find(class_ = 'page')
            pages_js.clear() 
            content_all += str(article).decode('utf8')
            
        additions.loc[idx,'content'] = content_all
    break

#%% 中国证券网
url = 'http://news.cnstock.com/news/sns_yw/index.html'
web_url = 'http://www.cnstock.com'

response = requests.get(url,headers = header_generator())
content = response.content
soup = BeautifulSoup(content,'html.parser')
content_titles = soup.find('ul',class_ = 'new-list article-mini')
news_time = [each.text[1:-1] for each in content_titles.find_all('span',class_ = 'time')]
hrefs = [each.attrs['href'] for each in content_titles.find_all('a')]
titles = [each.attrs['title'] for each in content_titles.find_all('a')]

news_df = pd.DataFrame([titles,news_time,hrefs],index = ['title',
                       'news_time','href']).T
    
additions = news_df


for idx,row in additions.iterrows():
    break
    link = row['href']
    link_response = requests.get(link,headers = header_generator())
    tmp_soup = BeautifulSoup(link_response.content,'html.parser')
    
    content = tmp_soup.find('div',class_ = 'content')
    imgs = content.find_all('img')
    
    for img in imgs:
        img.attrs['src'] = urlparse.urljoin(web_url,img.attrs['src'])
        
    content = str(content).decode('utf8')
    
    additions.loc[idx,'content'] = content
    break

#%% 证券时报网
stcn_url = 'http://news.stcn.com/'
response = requests.get(stcn_url,headers = header_generator())
content = response.content
soup = BeautifulSoup(content)
content_titles = soup.find_all(class_ = 'tit')[:100]
news_time = soup.find_all(class_ = 'sj')[:100]

titles = [each.find('a').text for each in content_titles]
hrefs = [each.find('a').attrs['href'] for each in content_titles]
news_time = [each.text[:10] + ' ' + each.text[10:] for each in news_time]

news_df = pd.DataFrame([titles,news_time,hrefs],index = ['title',
    'news_time','href']).T
 
additions = news_df
additions['news_source'] = ''
 
for idx,row in additions.iterrows():
    break
    link = row['href']
    link_response = requests.get(link,headers = header_generator())
    tmp_soup = BeautifulSoup(link_response.content,'html.parser')
    content = tmp_soup.find(class_ = 'txt_con')
    content = str(content).decode('utf8')
    
    
    additions.loc[idx,'content'] = content
    break

#%% 人民网
url = 'http://finance.people.com.cn/index1.html'
web_url = 'http://finance.people.com.cn/'
response = requests.get(url,headers = header_generator())
response.encoding = response.apparent_encoding
content = response.text

soup = BeautifulSoup(content,'html.parser')
related = soup.find_all('div',class_ = 'left w310')

for each in related:
    if each.find('h2'):
        if each.find('h2').text == u'宏观':
            hrefs = each
            break
        
hrefs = [each.attrs['href'] for each in hrefs.find_all('a')]
hrefs = [urlparse.urljoin(url,each) for each in hrefs]
        
news_df = pd.DataFrame([hrefs],index = ['href']).T

additions = news_df
additions['news_source'] = ''

for idx,row in additions.iterrows():
    break
    link = row['href']
    
    response = requests.get(link,headers = header_generator())
    response.encoding = response.apparent_encoding
    tmp_soup = BeautifulSoup(response.text,'html.parser')
    
    header = tmp_soup.find('div',class_ = 'clearfix w1000_320 text_title')
    title = header.find('h1').text.replace(u'\xa0',' ')
    news_time = header.find(class_ = 'fl').text[:16]
    news_time = news_time.replace(u'年','-')
    news_time = news_time.replace(u'月','-')
    news_time = news_time.replace(u'日',' ') 
    
    content = tmp_soup.find(class_ = 'box_con')

    pics = content.find_all('img')
    for pic in pics:
        pic['src'] = urlparse.urljoin(web_url,pic['src'])
    content = str(content).decode('utf8').replace(u'　',u' ').replace(u'\xa0',u' ')
    additions.loc[idx,'title'] = title
    additions.loc[idx,'news_time'] = news_time
    additions.loc[idx,'content'] = content
        
    break

#%% 人民网热点新闻
url = 'http://finance.people.com.cn/'

response = requests.get(url,headers = header_generator())
response.encoding = response.apparent_encoding
content = response.text

soup = BeautifulSoup(content,'html.parser')
related = soup.find('div',class_ = 'title mt15')
related1 = related.find('a')

href = urlparse.urljoin(url,related1.attrs['href'])
title = related1.text
        
news_df = pd.DataFrame([[title,href]],columns = ['title','href'])

additions = news_df
additions['news_source'] = u'人民网'
additions['if_header'] = 1

for idx,row in additions.iterrows():
    break
    link = row['href']
    
    response = requests.get(link,headers = header_generator())
    response.encoding = response.apparent_encoding
    tmp_soup = BeautifulSoup(response.text,'html.parser')
    
    header = tmp_soup.find('div',class_ = 'clearfix w1000_320 text_title')
    title = header.find('h1').text.replace(u'\xa0',' ')
    news_time = header.find(class_ = 'fl').text[:16]
    news_time = news_time.replace(u'年','-')
    news_time = news_time.replace(u'月','-')
    news_time = news_time.replace(u'日',' ') 
    
    content = tmp_soup.find(class_ = 'box_con')


    pics = content.find_all('img')
    for pic in pics:
        pic['src'] = urlparse.urljoin(web_url,pic['src'])
    content = str(content).decode('utf8').replace(u'　',u' ').replace(u'\xa0',u' ')
    additions.loc[idx,'news_time'] = news_time
    additions.loc[idx,'content'] = content
        
    break

#%% 证券时报网热点

stcn_url = 'http://news.stcn.com/'
response = requests.get(stcn_url,headers = header_generator())
content = response.content
soup = BeautifulSoup(content,'html.parser')
hot_news = soup.find(class_ = 'hotNews')

href = hot_news.dt.a.attrs['href']
title = hot_news.dd.text
news_time = hot_news.find(class_ = 'sj')
news_time = news_time.text[:10] + ' ' + news_time.span.text

news_df = pd.DataFrame([[title,news_time,href]],columns = ['title',
    'news_time','href'])
 
additions = news_df
additions['news_source'] = ''
 
for idx,row in additions.iterrows():
    break
    link = row['href']
    link_response = requests.get(link,headers = header_generator())
    tmp_soup = BeautifulSoup(link_response.content,'html.parser')
    content = tmp_soup.find(class_ = 'txt_con')
    content = str(content).decode('utf8')
    
    
    additions.loc[idx,'content'] = content
    break

#%% 中证网热点

zzw_url = 'http://www.cs.com.cn/xwzx/'

response = requests.get(zzw_url,headers = header_generator())

# parse titles
content = response.content
soup = BeautifulSoup(content,'html.parser')

top_news = soup.find(class_ = 'topnews hidden')
title = top_news.h1.text
href = urlparse.urljoin(zzw_url,top_news.h1.a.attrs['href'])

news_df = pd.DataFrame([[title,href]],columns = ['title','href'])

additions = news_df
additions['content'] = None
additions['news_source'] = u'中证网'

    
for idx,row in additions.iterrows():
    break
    link = row['href']
    
    link_response = requests.get(link,headers = header_generator())
    
    encoding = requests.utils.get_encodings_from_content(link_response.text)
    link_response.encoding = encoding[0]
    
    # parse content
    content = link_response.text
    tmp_soup = BeautifulSoup(content,'html.parser')
    info = tmp_soup.find(class_ = 'info')
    news_time = info.find('em').text
    article = tmp_soup.find('div',class_ = 'article-t hidden')
    
    # 解析总页数
    js_script = article.find(class_ = 'page').text
    count_page_str = re.findall(r'var countPage = (\d+)',js_script)
    count_page = int(count_page_str[0])
    
    # 剔除javascript
    pages_js = article.find(class_ = 'page')
    pages_js.clear() 
    
    if count_page == 1:        
        additions.loc[idx,'content'] = str(article).decode('utf8')
    else:
        link_compoent_list = link.split('.')
        last_file = link_compoent_list[-2]
        pages_list = [last_file + '_' + str(i) for i in range(1,count_page)]
        
        other_links = []
        for i in range(0,count_page - 1):
            link_compoent_list[-2] = pages_list[i]
            other_links.append('.'.join(link_compoent_list))
        
        content_all = str(article).decode('utf8')
        
        for other_link in other_links:
            link_response = requests.get(other_link,headers = header_generator())            
            encoding = requests.utils.get_encodings_from_content(link_response.text)
            link_response.encoding = encoding[0]    
            content = link_response.text
            tmp_soup = BeautifulSoup(content,'html.parser')
            article = tmp_soup.find('div',class_ = 'article-t hidden')
            pages_js = article.find(class_ = 'page')
            pages_js.clear() 
            content_all += str(article).decode('utf8')
            
        additions.loc[idx,'content'] = content_all
    break

#%% 中国证券网热点
url = 'http://www.cnstock.com/'
web_url = 'http://www.cnstock.com'

response = requests.get(url,headers = header_generator())
content = response.content
soup = BeautifulSoup(content,'html.parser')

header_topic = soup.find(class_ = 'hd-topic')
href = header_topic.h1.a.attrs['href']
title = header_topic.h1.a.text

news_df = pd.DataFrame([[title,href]],columns = ['title','href'])
    
additions = news_df


for idx,row in additions.iterrows():
    break
    link = row['href']
    link_response = requests.get(link,headers = header_generator())
    tmp_soup = BeautifulSoup(link_response.content,'html.parser')
    
    content = tmp_soup.find('div',class_ = 'content')
    imgs = content.find_all('img')
    
    for img in imgs:
        img.attrs['src'] = urlparse.urljoin(web_url,img.attrs['src'])
        
    content = str(content).decode('utf8')
    
    news_time = tmp_soup.find(class_ = 'timer').text
    additions.loc[idx,'content'] = content
    break