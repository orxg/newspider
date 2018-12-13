# -*- coding: utf-8 -*-
"""
Created on Tue Jul 10 09:12:41 2018

@author: ldh
"""

# SQLs.py

SQL_MERGE_MACRO_NEWS = '''
MERGE INTO news_macro_base as T
USING news_macro_source as S
ON T.unique_flag = S.unique_flag
WHEN MATCHED
THEN UPDATE SET
content = S.content,update_datetime = S.update_datetime,news_source = S.news_source
WHEN NOT MATCHED BY TARGET
THEN INSERT
(title,news_time,href,content,update_datetime,news_source,unique_flag,if_header)
VALUES
(S.title,S.news_time,S.href,S.content,S.update_datetime,S.news_source,S.unique_flag,S.if_header);
'''


SQL_MERGE_MACRO_NEWS_FOR_HEADER = '''
MERGE INTO news_macro_base as T
USING news_macro_source as S
ON T.unique_flag = S.unique_flag
WHEN MATCHED
THEN UPDATE SET
content = S.content,update_datetime = S.update_datetime,news_source = S.news_source,
if_header = S.if_header,news_time = S.news_time
WHEN NOT MATCHED BY TARGET
THEN INSERT
(title,news_time,href,content,update_datetime,news_source,unique_flag,if_header)
VALUES
(S.title,S.news_time,S.href,S.content,S.update_datetime,S.news_source,S.unique_flag,S.if_header);
'''

SQL_MERGE_MACRO_NEWS_FOR_ALL = '''
MERGE INTO news_macro_all as T
USING news_macro_source as S
ON (T.unique_flag = S.unique_flag AND T.news_source = S.news_source)
WHEN NOT MATCHED BY TARGET
THEN INSERT
(title,news_time,update_datetime,news_source,unique_flag)
VALUES
(S.title,S.news_time,S.update_datetime,S.news_source,S.unique_flag);
'''

SQL_GET_MACRO_NEWS_TOP = '''
SELECT TOP 1000
unique_flag,news_source
FROM news_macro_all
ORDER BY news_time DESC
'''

SQL_MERGE_MACRO_NEWS_REPEAT_TIMES = '''
MERGE INTO news_macro_base as T
USING news_repeat as S
ON T.unique_flag = S.unique_flag
WHEN MATCHED 
THEN UPDATE SET
repeat_times = S.repeat_times;
'''

SQL_GET_MACRO_NEWS_HEADER_TOP = '''
SELECT TOP 1000
unique_flag,news_source
FROM news_macro_all
ORDER BY news_time DESC
'''
