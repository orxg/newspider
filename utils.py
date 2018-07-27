# -*- coding: utf-8 -*-
"""
Created on Fri Jul 20 15:42:21 2018

@author: ldh
"""

# utils.py

import datetime as dt
import random

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