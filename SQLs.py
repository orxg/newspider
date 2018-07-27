# -*- coding: utf-8 -*-
"""
Created on Tue Jul 10 09:12:41 2018

@author: ldh
"""

# SQLs.py

SQL_MERGE_MACRO_NEWS = '''
MERGE INTO news_macro as T
USING news_macro_source as S
ON T.title = S.title
WHEN MATCHED
THEN UPDATE SET
content = S.content,update_datetime = S.update_datetime,news_source = S.news_source
WHEN NOT MATCHED BY TARGET
THEN INSERT
(title,news_time,href,content,update_datetime,news_source)
VALUES
(S.title,S.news_time,S.href,S.content,S.update_datetime,S.news_source);
'''