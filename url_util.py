# -*- coding: UTF-8 -*-
"""
@author: kongxiangshuai
@file: url_util.py
@time: 2021/10/06
"""
import urllib.parse


def parse_url(url):
    return '/'.join(map(url_chinese, url.split('/')))


def url_chinese(s):
    if is_chinese(s):
        return urllib.parse.quote(s)
    return s


def is_chinese(s):
    for ch in s:
        if u'\u4e00' <= ch <= u'\u9fff':
            return True
    return False
