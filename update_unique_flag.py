# -*- coding: utf-8 -*-
"""
Created on Thu Aug 09 15:05:27 2018

@author: ldh
给现在的数据库添加unique flag内容
"""

# update_unique_flag.py
from sqlalchemy.types import VARCHAR,NVARCHAR,DATETIME
from database_engine import DatabaseEngine
import pandas as pd
import pymssql

conn = pymssql.connect(server = '172.19.62.10',user = 'financial_xiaoyi',
                       password = 'fs95536!',database = 'sds209635243_db')
cursor = conn.cursor()

data = pd.read_sql('''SELECT * FROM [sds209635243_db].[dbo].[news_macro]   where news_source = '人民网' ''',
                   conn)

punctuation_list = ['\xa0'.decode('latin1'),u'?',u'.',u',',u'!',u':',u'"',u' ',u'…',u'%',u'<',u'>',
                           u'？', u'。',u'，',u'！',u'：',u'”',u'“',u'、',u'《',
                           u'》']

def delete_punctuations(title):
    new_title = title
    for punc in punctuation_list:
        new_title = new_title.replace(punc,'')
    return new_title
data['unique_flag'] = data['title'].apply(delete_punctuations)
data = data[['title','unique_flag']]


engine_obj = DatabaseEngine('xiaoyi')
engine = engine_obj.get_engine()
session = engine_obj.get_session()
data.to_sql('news_macro_source1',engine,
                      index = False,if_exists = 'replace',
                 dtype = {'title':VARCHAR(256),
                          'unique_flag':VARCHAR(256)})
    
    
tmp_sql = '''
MERGE INTO news_macro as T
USING news_macro_source1 as S
ON T.title = S.title
WHEN MATCHED
THEN UPDATE SET
unique_flag = S.unique_flag;
'''

session.execute(tmp_sql)
session.commit()



