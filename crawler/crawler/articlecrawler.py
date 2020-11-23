#!/usr/bin/env python
# -*- coding: utf-8, euc-kr -*-
import os
import platform
from datetime import date, datetime
import re

import concurrent
import time 
import json
import logging

import requests
from bs4 import BeautifulSoup
import pyarrow as pa
from pyarrow import csv, parquet
from s3fs.core import S3FileSystem, aiobotocore

from config import CONFIG
from crawler.exceptions import *
from crawler.writer import Writer
from crawler.schema import generate_schema
from crawler.articleparser import ArticleParser
from utils import BackOff, logging_time, ConnectionStore, DataManager, get_document


NAVER_URL = CONFIG['naver_news']['urls']['article_url']

logger = logging.getLogger()

class ArticleCrawler(object):
    def __init__(self, connection:ConnectionStore):
        self.categories = {'정치': 100, '경제': 101, '사회': 102, '생활문화': 103, '세계': 104, 'IT과학': 105, '오피니언': 110}
        self.connection = connection
        self.selected_categories = []
        self.date = {'year': 0, 'month': 0, 'day': 0}
        self.user_operating_system = str(platform.system())

    def set_category(self, *args):
        for key in args:
            if self.categories.get(key) is None:
                raise InvalidCategory(key)
        self.selected_categories = args

    def set_date_range(self, year, month, day):
        args = [year, month, day]
        if month < 1 or month > 12:
            raise InvalidMonth(month)
        if day < 1 or day > 31:
            raise InvalidDay(day)
        for key, date in zip(self.date, args):
            self.date[key] = date
        logger.info(f"TARGET DATE: {self.date}")

    def get_setting_date(self):
        if self.date.get('year') is None:
            logger.error("SET THE target date first")
            return
        target_date = date(**self.date)
        return target_date

    def make_news_page_url(self, category_url, year, month, day):
        made_urls = []
        
        if len(str(month)) == 1:
            month = "0" + str(month)
        if len(str(day)) == 1:
            day = "0" + str(day)
        category_date_url = category_url + str(year) + str(month) + str(day)
        total_url = category_date_url + "&page=10000"
        document = get_document(total_url)
        totalpage = ArticleParser.find_news_totalpage(document)

        for page in range(1, totalpage + 1):
            made_urls.append(category_date_url + "&page=" + str(page))
            
        return made_urls


    @staticmethod
    def get_url_data(url, max_tries=10):
        remaining_tries = int(max_tries)
        while remaining_tries > 0:
            try:
                return requests.get(url)
            except ConnectionError:
                time.sleep(60)
            remaining_tries = remaining_tries - 1
        raise ResponseTimeout()

    
    def make_crawling_log_by_page(self, page_url):
        logger.info(f'Make Crawling Log By Page {page_url}')
        columns_list = ['news_id', 'article_url', 'retrieve_datetime', 'created_date']
        document = get_document(page_url)
        news_id_url_list = ArticleParser.get_target_news_id_url(document)

        now = datetime.utcnow()
        target_date = self.get_setting_date()
        values_list = [id_url + [now, target_date] for id_url in news_id_url_list]
        values_list = [DataManager.values_query_formmater(values_list=value) for value in values_list]

        self.connection.upsert(table_name = 'news_crawling_log',
                                columns_list = columns_list,
                                values_list = values_list,
                                action_on_conflict = 'nothing')
                                
        return page_url

    def make_crawling_log(self, category_name):
        if self.date['year'] == 0:
            logger.error('Set target date before running')
            return
    
        logger.info(f'Making Crawling Log starts {category_name}')
        category_url = NAVER_URL + str(self.categories.get(category_name)) + '&date='
        day_urls = self.make_news_page_url(category_url, **self.date)

        with concurrent.futures.ThreadPoolExecutor() as pool:
            future_workers = []
            for page_url in day_urls:
                future_workers.append(
                    pool.submit(
                        self.make_crawling_log_by_page,
                        page_url
                    )
                )
            for worker in concurrent.futures.as_completed(future_workers):
                worker.result()

    

    # def crawling(self, category_name):
    #     # Multi Process PID
    #     logger.info(category_name + " PID: " + str(os.getpid()))

    #     writer = Writer(category_name=category_name, date=self.date)
    #     wcsv = writer.get_writer_csv()
    #     wcsv.writerow(["date", "time", "category", "company", "author", "headline", "sentence", "content_url", "image_url"])
        

    #     # 기사 URL 형식
    #     url = "http://news.naver.com/main/list.nhn?mode=LSD&mid=sec&sid1=" + str(
    #         self.categories.get(category_name)) + "&date="

    #     # start_year년 start_month월 ~ end_year의 end_month 날짜까지 기사를 수집합니다.
    #     day_urls = self.make_news_page_url(url, self.date['year'], self.date['month'], self.date['day'])
    #     logger.info(category_name + " Urls are generated")
    #     logger.info("The crawler starts")

    #     with concurrent.futures.ThreadPoolExecutor() as pool:
    #         futureWorkers = []
    #         for URL in day_urls:
    #             futureWorkers.append(pool.submit(
    #                 self.get_page_and_write_row,
    #                 category_name,
    #                 writer,
    #                 URL,
    #             ))
    #         for future in concurrent.futures.as_completed(futureWorkers):
    #             print(future.result())
    #     writer.close()


    # def get_page_and_write_row(self, category_name, writer, URL):
    #     news_date = self.get_date_from_URL(URL)

    #     request = self.get_url_data(URL)
    #     document = BeautifulSoup(request.content, 'html.parser')

    #     # html - newsflash_body - type06_headline, type06
    #     # 각 페이지에 있는 기사들 가져오기
    #     post_temp = document.select('.newsflash_body .type06_headline li dl')
    #     post_temp.extend(document.select('.newsflash_body .type06 li dl'))

    #     # 각 페이지에 있는 기사들의 url 저장
    #     post = []
    #     for line in post_temp:
    #         post.append(line.a.get('href'))  # 해당되는 page에서 모든 기사들의 URL을 post 리스트에 넣음
    #     del post_temp

    #     for content_url in post:  # 기사 URL
    #         # 크롤링 대기 시간
    #         sleep(0.01)

    #         # 기사 HTML 가져옴
    #         request_content = self.get_url_data(content_url)
    #         try:
    #             document_content = BeautifulSoup(request_content.content, 'html.parser')
    #         except:
    #             continue


    #         try:
    #             # 기사 제목 가져옴
    #             text_headline = ArticleParser.get_headline_from_document(document_content)
    #             # 기사 본문 가져옴
    #             text_sentence = ArticleParser.get_sentence_from_document(document_content)
    #             # 기사 언론사 가져옴
    #             text_company = ArticleParser.get_company_from_document(document_content)
    #             # 기사 이미지 가져옴
    #             image_url = ArticleParser.get_imgURL_from_document(document_content)
    #             # 기사 시간 가져옴
    #             news_time = ArticleParser.get_time_from_document(document_content)
    #             # 기자 가져옴
    #             author_name = ArticleParser.find_author(text_sentence)

    #             # CSV 작성
    #             wcsv = writer.get_writer_csv()
    #             wcsv.writerow([news_date, news_time, category_name, text_company, author_name, text_headline, text_sentence, content_url, image_url])
                
    #             del text_company, text_sentence, text_headline
    #             del image_url
    #             del request_content, document_content
    #             return("DONE")
                
    #         except Exception as e:  # UnicodeEncodeError ..
    #             # wcsv.writerow([ex, content_url])
    #             del request_content, document_content
    #             return(f"ERROR : {e}")

    # def date_loader(self):
    #     with open("/Users/jungyulyang/programming/hell-news/config/credential.json") as json_file:
    #         json_data = json.load(json_file)

    #     db = pymysql.connect(
    #         user = json_data['user'],
    #         passwd = json_data['passwd'],
    #         host = json_data['host'],
    #         charset = json_data['charset']
    #     )

    #     cursor = db.cursor(pymysql.cursors.DictCursor)
    #     cursor.execute('USE news_crawling')

    #     sql = "select dates from only_date"
    #     cursor.execute(sql)
    #     rows = cursor.fetchall()
    #     sql_data = str(rows[0])

    #     numbers = re.findall("\d+", sql_data)
    #     sql_year = int(numbers[0])
    #     sql_month = int(numbers[1])
    #     sql_day = int(numbers[2])

    #     sql = "delete from only_date limit 1"
    #     cursor.execute(sql)
    #     db.commit()

    #     return sql_year, sql_month, sql_day


    # def start(self):
    #     # MultiProcess 크롤링 시작
    #     with concurrent.futures.ProcessPoolExecutor() as process:
    #         futureWorkers = []
    #         for category_name in self.selected_categories:
    #             futureWorkers.append(process.submit(
    #                 self.crawling,
    #                 category_name
    #             ))
    #         for future in concurrent.futures.as_completed(futureWorkers):
    #             print(future.result())

    def today_date_loader(self):
        today = date.today()
        numbers = re.findall("\d+", str(today))
        today_year = int(numbers[0])
        today_month = int(numbers[1])
        today_day = int(numbers[2])
        print(today_year, today_month, today_day)

        return today_year, today_month, today_day
            
if __name__ == "__main__":
    postgre_connection = ConnectionStore(CONFIG["postgresql_database"],
                                   CONFIG["postgresql_host"],
                                   CONFIG["postgresql_port"],
                                   CONFIG["postgresql_user"],
                                   CONFIG["postgresql_password"],)
    Crawler = ArticleCrawler(connection=postgre_connection)
    Crawler.set_category("정치",)

    # 날짜 하나씩 불러올때
    # sql_year, sql_month, sql_day = Crawler.date_loader()
    Crawler.set_date_range(2020, 11, 15)
    Crawler.today_date_loader()
    print(Crawler.get_setting_date())
    # #오늘 날짜
    # today_year, today_month, today_day = Crawler.today_date_loader()
    # Crawler.set_date_range(today_year, today_month, today_day)

    # 날짜 설정
    # Crawler.set_date_range(2020, 8, 15)

    # Crawler.make_crawling_log('정치')
    