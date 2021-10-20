# -*- coding: UTF-8 -*-
"""
@author: kongxiangshuai
@file: baike_parser.py
@time: 2021/10/06
"""
import json
import re
from spider import HTMLParser, SpiderScheduler
from mysql_util import get_connection
from spider import Outer

from bs4 import BeautifulSoup


class MysqlOuter(Outer):

    insert_sql = "insert into baike (`titile`, `content`, `url`, `type`, `image`, `link`) values (%s,%s,%s,%s,%s,%s)"

    query_sql = "select * from baike where url = '%s'"

    def __init__(self, batch_size=50):
        self.conn = get_connection()
        self.cursor = self.conn.cursor()
        self.batch_size = batch_size
        self.data = []

    def output(self, data):
        if data:
            data = json.loads(data)
            url = data['url']
            if not self._query_by_url(url):
                self.data.append((data['title'], data['content'], data['url'], data['type'], data['image'], json.dumps(data['link'], ensure_ascii=False)))
                if len(self.data) >= self.batch_size:
                    self._insert()

    def _query_by_url(self, url):
        sql = self.query_sql % url
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        if len(result) > 0:
            return result[0]

    def _insert(self):
        print(self.data)
        self.cursor.executemany(self.insert_sql, self.data)
        self.data = []
        self.conn.commit()

    def close(self):
        self._insert()
        self.cursor.close()
        self.conn.close()


class Baike(HTMLParser):

    def __init__(self):
        self.URL_PREFIX = "https://baike.baidu.com"
        self.urls = set()

    def parse(self, text):
        self.urls = set()
        soup = BeautifulSoup(text, 'html.parser')
        if not soup:
            return
        result_data = self._parse_data(soup)
        return list(self.urls), result_data

    @staticmethod
    def _clear_baike_content(string):
        return re.sub(r'\[(-|\d)*]|\n|\xa0', '', string).strip()

    def _add_url(self, child_url):
        tmp_url = child_url.replace(self.URL_PREFIX, '')
        if tmp_url.startswith('/item'):
            self.urls.add(self.URL_PREFIX + tmp_url)

    def _parse_data(self, soup):
        title = soup.find('dd', class_='lemmaWgt-lemmaTitle-title J-lemma-title').find('h1')
        content = soup.find('div', class_='lemma-summary')
        url = soup.find('meta', attrs={"property": "og:url"})['content'].split('?')[0]
        og_type = soup.find('meta', attrs={"property": "og:type"})['content']
        og_image = soup.find('meta', attrs={"property": "og:image"})['content']
        if content:
            result_dict = {
                'title': title.get_text(),
                'content': self._clear_baike_content(content.get_text()),
                'url': url,
                'type': og_type,
                'image': og_image,
                'link': []
            }
            for ele in content.find_all('a', attrs={}):
                child_url = ele.get('href')
                content_field = ele.get_text()
                if child_url:
                    self._add_url(child_url)
                    child_dict = {
                        'content': content_field,
                        'url': self.URL_PREFIX + child_url
                    }
                    result_dict['link'].append(child_dict)
            return json.dumps(result_dict, ensure_ascii=False)


def main(url):
    baike_parser = Baike()
    ss = SpiderScheduler(baike_parser, max_url=1000)
    mysql_outer = MysqlOuter(batch_size=50)
    ss.add_outer(mysql_outer)
    ss.craw(url)


if __name__ == '__main__':
    # root_url = 'https://baike.baidu.com/item/君九龄'
    # root_url = 'https://baike.baidu.com/item/国子监来了个女弟子/53480412'
    # root_url = 'https://baike.baidu.com/item/%E5%8A%9F%E5%8B%8B/24265662'
    root_url = 'https://baike.baidu.com/item/%E6%89%AB%E9%BB%91%E9%A3%8E%E6%9A%B4/24256736'
    main(root_url)

