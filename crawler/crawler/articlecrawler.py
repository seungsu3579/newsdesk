#!/usr/bin/env python
# -*- coding: utf-8, euc-kr -*-
import os
import platform
from datetime import date, datetime
import re

import concurrent
import time 
import json
import csv
import logging
import tempfile

import requests
from bs4 import BeautifulSoup
import pyarrow as pa
import boto3
from s3fs.core import S3FileSystem, aiobotocore

from config import CONFIG
from crawler.exceptions import *
from crawler.schema import generate_schema
from crawler.articleparser import ArticleParser
from utils import BackOff, logging_time, ConnectionStore, DataManager, get_document


NAVER_URL = CONFIG['naver_news']['urls']['article_url']
S3_PROFILE_NAME = CONFIG['s3_configure']['profile_name']
logger = logging.getLogger(__name__)
logger.setLevel('INFO')

class ArticleCrawler(object):
    def __init__(self, config, connection:ConnectionStore):
        self.categories = {'정치': 100, '경제': 101, '사회': 102, '생활문화': 103, '세계': 104, 'IT과학': 105, '오피니언': 110}
        self.connection = connection
        self.s3_profile = config['s3_configure']['profile_name']
        self.s3_bucket = config['s3_configure']['content_bucket']
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

    def get_data_from_article_url(self, news_id:str, article_url:str) -> dict:
        
        document = get_document(article_url)

        article_headline = ArticleParser.get_headline_from_document(document)
        img_url = ArticleParser.get_imgURL_from_document(document)
        created_datetime = ArticleParser.get_datetime_from_document(document)
        article_company = ArticleParser.get_company_from_document(document)
        reporter_name = ArticleParser.get_author_name(document)
        content = ArticleParser.get_sentence_from_document(document)
        content_length = len(content) if content is not None else 0
        category = ArticleParser.get_category_name(document)

        return dict(news_id=news_id,
                    article_url=article_url,
                    created_datetime=created_datetime, 
                    category=category, 
                    article_headline=article_headline,
                    article_company=article_company,
                    reporter_name=reporter_name,
                    article_length=content_length,
                    image_url=img_url,
                    content=content)
    
    def get_download_target(self, category:str) -> list:
        category_num = self.categories.get(category, None)
        if category_num is None:
            raise Exception(message='must input collect category')
        query = f"""
                SELECT news_id, article_url FROM news_crawling_log
                WHERE created_date = '{self.get_setting_date()}'
                  AND news_id LIKE '{category_num}-%'
                  AND download_status = 'NOT DONE'
                """
        target_news = self.connection.execute_query(query).fetchall()
        return target_news
    
    def crawl_category(self, category:str) -> (list, list):
        target_news = self.get_download_target(category)[:50]
        metadata_list = list()
        content_list = list()
        with concurrent.futures.ThreadPoolExecutor() as pool:
            future_workers = []
            for news_id, news_url in target_news:
                future_workers.append(
                    pool.submit(self.get_data_from_article_url, news_id, news_url)
                )
            for worker in concurrent.futures.as_completed(future_workers):
                result = worker.result()
                content_list.append(dict(news_id=result.get('news_id'), 
                                         content=result.get('content')))
                del result['content']
                metadata_list.append(result)

        return content_list, metadata_list

    def upload_s3_csv(self, category:str, content_list:list):
        session = boto3.Session(profile_name=S3_PROFILE_NAME)
        s3 = session.client('s3')
        date = self.get_setting_date()
        s3_fname = f"{category}/{date}.csv"
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv',encoding='utf-8', newline='') as fp:
            writer = csv.writer(fp)
            writer.writerow(["news_id", "content"])
            for data in content_list:
                writer.writerow([data.get('news_id'), data.get('content')])
            s3.upload_file(fp.name, self.s3_bucket, s3_fname)
        return 
    
    
    def crawl_all_categries(self, categories:str) -> str:
        for category in categories:
            logger.info(f'[CRAWLING] {category} START')
            content_list, metadata_list = self.crawl_category(category)
            logger.info(f'[CRAWLING] {category} DOWNLOAD DATA IS DONE')
            columns_list = metadata_list[0].keys()
            metadata_list = DataManager.get_query_format_from_dict(columns_list=columns_list,
                                                                values_list_dict=metadata_list)
            self.connection.upsert(table_name='news_metadata', columns_list=columns_list, values_list=metadata_list, action_on_conflict='update')
            logger.info(f'[CRAWLING] {category} UPDATE META DATA IS DONE')
            self.upload_s3_csv(category, content_list)
            logger.info(f'[CRAWLING] {category} UPLOAD CONTENT DATA IS DONE')




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
    Crawler = ArticleCrawler(config=CONFIG, connection=postgre_connection)

    # 날짜 하나씩 불러올때
    # sql_year, sql_month, sql_day = Crawler.date_loader()
    Crawler.set_date_range(2020, 11, 14)
    Crawler.today_date_loader()
    Crawler.crawl_all_categries(['경제','정치'])

    # #오늘 날짜
    # today_year, today_month, today_day = Crawler.today_date_loader()
    # Crawler.set_date_range(today_year, today_month, today_day)

    # 날짜 설정
    # Crawler.set_date_range(2020, 8, 15)

    # Crawler.make_crawling_log('정치')
    