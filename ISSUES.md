1. ths 有错误（解决)
2. cnstock  merge 语句提示有重复项（解决)


3. 报错
Traceback (most recent call last):
  File "C:\Anaconda2\lib\threading.py", line 801, in __bootstrap_inner
    self.run()
  File "C:\Anaconda2\lib\threading.py", line 754, in run
    self.__target(*self.__args, **self.__kwargs)
  File "E:\Projects\news_macro\spider_base.py", line 68, in spider_data
    status_code = self._get_titles_response()
  File "E:\Projects\news_macro\spider_base.py", line 104, in _get_titles_respo
e
    self.titles_response = requests.get(self.title_url,headers = self.header_g
erator())
  File "C:\Anaconda2\lib\site-packages\requests\api.py", line 72, in get
    return request('get', url, params=params, **kwargs)
  File "C:\Anaconda2\lib\site-packages\requests\api.py", line 58, in request
    return session.request(method=method, url=url, **kwargs)
  File "C:\Anaconda2\lib\site-packages\requests\sessions.py", line 518, in req
st
    resp = self.send(prep, **send_kwargs)
  File "C:\Anaconda2\lib\site-packages\requests\sessions.py", line 639, in sen
    r = adapter.send(request, **kwargs)
  File "C:\Anaconda2\lib\site-packages\requests\adapters.py", line 502, in sen
    raise ConnectionError(e, request=request)
ConnectionError: HTTPConnectionPool(host='news.10jqka.com.cn', port=80): Max r
ries exceeded with url: /cjzx_list/index_1.shtml (Caused by NewConnectionError
<requests.packages.urllib3.connection.HTTPConnection object at 0x000000000A7C3
0>: Failed to establish a new connection: [Errno 11001] getaddrinfo failed',))