# -*- coding: UTF-8 -*-
"""
@author: kongxiangshuai
@file: spider.py
@time: 2021/10/04
"""
import os.path
import pickle
import random
import time
import urllib.request
from queue import SimpleQueue
from url_util import parse_url

"""
调度器 -> URL管理器 -> 下载器 -> 解析器 -> 应用
"""


class Cache(object):

    def load(self):
        pass

    def close(self):
        pass

    def has_key(self, k):
        pass

    def put(self, k, v):
        pass

    def get(self, k):
        pass


class FileCache(Cache):

    CACHE_FILE_NAME = 'tmp.file'

    def __init__(self):
        self.cache_dict = {}

    def load(self):
        if os.path.exists(self.CACHE_FILE_NAME):
            with open(self.CACHE_FILE_NAME, 'rb') as f:
                self.cache_dict = pickle.load(f)
                print(['load cache ...', len(self.cache_dict)])

    def close(self):
        with open(self.CACHE_FILE_NAME, 'wb') as f:
            pickle.dump(self.cache_dict, f)

    def has_key(self, k):
        return k in self.cache_dict.keys()

    def get(self, k):
        return self.cache_dict.get(k)

    def put(self, k, v):
        self.cache_dict[k] = v


class PageDownloader(object):
    """
    网页下载器
    """

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/94.0.4606.61 Safari/537.36'
    }

    def __init__(self, cache=None):
        self.cache_dict = {}
        if not cache:
            cache = FileCache()
        self.cache = cache
        self.cache.load()
        self.down_count = 1

    def _url_request(self, url):
        req = urllib.request.Request(url=parse_url(url), headers=self.headers, method='GET')
        response = urllib.request.urlopen(req)
        if response.getcode() != 200:
            return None
        time.sleep(random.uniform(1.0, 5.0))
        response_text = response.read().decode('utf-8')
        return response_text

    def download(self, url):
        if not url:
            return None

        if url in self.cache_dict.keys():
            print('read from cache ... ')
            return self.cache_dict[url]

        if self.cache.has_key(url):
            return self.cache.get(url)
        else:
            data = self._url_request(url)
            self.cache.put(url, data)
            if self.down_count % 50 == 0:
                self.cache.close()
            self.down_count += 1
            return data

    def close(self):
        self.cache.close()


class SpiderScheduler(object):

    def __init__(self, parser, max_url=1000, print_data=True):
        self.max_download_cnt = max_url
        self.urls = UrlManager()
        self.downloader = PageDownloader()
        self.parser = parser
        self.outer = []
        if print_data:
            out = PrintOuter()
            self.add_outer(out)

    def add_outer(self, outer):
        """添加输出处理

        :param outer:
        :return:
        """
        self.outer.append(outer)

    def craw(self, root_url):
        """执行查询

        :param root_url:
        :return:
        """
        count = 1
        self.urls.add_new_url(root_url)
        while self.urls.has_new_url():
            url = self.urls.get_new_url()
            html_response = self.downloader.download(url)
            new_urls, res_data = self.parser.parse(html_response)
            self.urls.add_new_urls(new_urls)
            for o in self.outer:
                o.output(res_data)

            if count % 10 == 0:
                print([count, self.urls.size()])

            if count > self.max_download_cnt:
                break

            count += 1

        self.close()

    def close(self):
        self.urls.close()
        self.downloader.close()
        for outer in self.outer:
            outer.close()


class UrlManager(object):
    """地址管理

    新URL地址队列

    """

    def __init__(self):
        self._new_urls = SimpleQueue()
        self._used_url = set()

    def add_new_url(self, url):
        if not url:
            return
        if not self._is_used(url):
            self._new_urls.put(url)
            self._used(url)

    def add_new_urls(self, urls):
        if not urls and len(urls) == 0:
            return
        for url in urls:
            self.add_new_url(url)

    def has_new_url(self):
        return not self._new_urls.empty()

    def get_new_url(self) -> str:
        """
        :rtype: string
        """
        return self._new_urls.get()

    def _used(self, url):
        self._used_url.add(url)

    def _is_used(self, url):
        return url in self._used_url

    def size(self):
        return self._new_urls.qsize()

    def close(self):
        pass


class HTMLParser(object):

    def parse(self, text):
        pass


class Outer(object):

    def output(self, data):
        pass

    def close(self):
        pass


class PrintOuter(Outer):

    def output(self, data):
        print(data)

    def close(self):
        pass
