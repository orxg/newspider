# -*- coding: utf-8 -*-
"""
Created on Fri Nov 03 15:18:17 2017

@author: ldh

提供数据库接口,数据库配置在etc.yaml中按照
相应的格式进行配置即可。创建引擎参数type_
为对应的服务器配置名。
"""

# database_engine.py

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import yaml
from consts import DB_STATE

with open('etc.yaml','r') as f:
    etc = yaml.load(f)

class DatabaseEngine():
    def __init__(self,type_):
        self.type_ = type_
        self.etc = etc[self.type_]
        self.engine = None
        
    def get_engine(self):
        self.engine = create_engine(DB_STATE.format(
                user = self.etc['user'],
                pws = self.etc['passwd'],
                server = self.etc['server'],
                db = self.etc['db']))        
        return self.engine
    
    def get_session(self):
        assert self.engine
        self.session = sessionmaker(bind = self.engine)()
        return self.session
    

        

