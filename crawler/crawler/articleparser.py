from datetime import datetime

from bs4 import BeautifulSoup
import requests
import re
from utils import DataManager


class ArticleParser(object):
    special_symbol = re.compile(r'[\{\}\[\]\/?,;:|\)*~`!^\-_+<>@\#$&▲▶◆◀■【】\\\=\(\'\"]')
    content_pattern = re.compile('본문 내용|TV플레이어| 동영상 뉴스|flash 오류를 우회하기 위한 함수 추가function  flash removeCallback|tt|앵커 멘트|xa0')

    @classmethod
    def clear_content(cls, text):
        # 기사 본문에서 필요없는 특수문자 및 본문 양식 등을 다 지움
        newline_symbol_removed_text = text.replace('\\n', '').replace('\\t', '').replace('\\r', '')
        special_symbol_removed_content = re.sub(cls.special_symbol, ' ', newline_symbol_removed_text)
        end_phrase_removed_content = re.sub(cls.content_pattern, '', special_symbol_removed_content)
        blank_removed_content = re.sub(' +', ' ', end_phrase_removed_content).lstrip()  # 공백 에러 삭제
        reversed_content = ''.join(reversed(blank_removed_content))  # 기사 내용을 reverse 한다.
        content = ''
        for i in range(0, len(blank_removed_content)):
            # reverse 된 기사 내용중, ".다"로 끝나는 경우 기사 내용이 끝난 것이기 때문에 기사 내용이 끝난 후의 광고, 기자 등의 정보는 다 지움
            if reversed_content[i:i + 2] == '.다':
                content = ''.join(reversed(reversed_content[i:]))
                break
        content = content.split(" ")
        for index, word in enumerate(content):
            if word == '기자' and len(content[index-1]) == 3:
                # 000 기자 라는 글자 본문에서 제외
                content = content[:index-1] + content[index+1:]
                break

        content = " ".join(content)
        return content
    
    @classmethod
    def parse_author_from_content(cls, text):
        newline_symbol_removed_text = text.replace('\\n', '').replace('\\t', '').replace('\\r', '')
        special_symbol_removed_content = re.sub(cls.special_symbol, ' ', newline_symbol_removed_text)
        end_phrase_removed_content = re.sub(cls.content_pattern, '', special_symbol_removed_content)
        blank_removed_content = re.sub(' +', ' ', end_phrase_removed_content).lstrip()  # 공백 에러 삭제
        reversed_content = ''.join(reversed(blank_removed_content))  # 기사 내용을 reverse 한다.
        for i in range(0, len(blank_removed_content)):
            # reverse 된 기사 내용중, ".다"로 끝나는 경우 기사 내용이 끝난 것이기 때문에 기사 내용이 끝난 후의 광고, 기자 등의 정보는 다 지움
            if reversed_content[i:i + 2] == '.다':
                break
            author_sentence = ''.join(reversed(reversed_content[:i]))

        # 다. 이후로 시작되는 문장에서 기자 찾기
        author_sentence = author_sentence.split(' ')
        for n, word in enumerate(author_sentence):
            if word == '기자':
                # '기자' 바로 앞이 기자 이름일것
                author = author_sentence[n-1]
                return author

        # 마지막 문장엣 기자가 없을경우 앞문장에서 기자 찾기
        author_sentence = blank_removed_content.split(' ')[:50]
        for n, word in enumerate(author_sentence):
            if word == '기자':
                author = author_sentence[n-1]
                return author

        return None

    @classmethod
    def clear_headline(cls, text):
        # 기사 제목에서 필요없는 특수문자들을 지움
        newline_symbol_removed_text = text.replace('\\n', '').replace('\\t', '').replace('\\r', '')
        special_symbol_removed_headline = re.sub(cls.special_symbol, '', newline_symbol_removed_text)
        return special_symbol_removed_headline

    @classmethod
    def find_news_totalpage(cls, document):
        # 당일 기사 목록 전체를 알아냄
        headline_tag = document.find('div', {'class': 'paging'}).find('strong')
        regex = re.compile(r'<strong>(?P<num>\d+)')
        match = regex.findall(str(headline_tag))
        return int(match[0])

    @staticmethod
    def make_news_id(article_url:str) -> str:
        
        aid_index = article_url.find("aid=")
        aid = article_url[aid_index+4:].split('&')[0]

        sid_index = article_url.find("sid1=")
        sid = article_url[sid_index+5:].split('&')[0]

        oid_index = article_url.find("oid=")
        oid = article_url[oid_index+4:].split('&')[0]

        # if not sid.isdigit() or not aid.isdigit() or not oid.isdigit():
        #     return None 

        news_id = str(sid) + '-' + str(oid) + '-' + str(aid)
        return news_id

    @staticmethod
    def get_target_news_id_url(document:BeautifulSoup) -> list:
        post_temp = document.select('.newsflash_body .type06_headline li dl')
        post_temp.extend(document.select('.newsflash_body .type06 li dl'))
        # 각 페이지에 있는 기사들의 url 저장
        news_id_list = []

        for line in post_temp:
            article_url = line.a.get('href')
            news_id = ArticleParser.make_news_id(article_url)
            if news_id is None:
                continue
            news_id_list.append([news_id, article_url])

        return news_id_list

    @staticmethod
    def get_date_from_URL(URL) -> str:
        regex = re.compile("date=(\\d+)")
        news_date = regex.findall(URL)[0]
        return news_date

    @staticmethod
    def get_headline_from_document(document:BeautifulSoup) -> str:
        tag_headline = document.find_all('h3', {'id': 'articleTitle'}, {'class': 'tts_head'})
        text_headline = ''  # 뉴스 기사 제목 초기화
        text_headline = text_headline + ArticleParser.clear_headline(
            str(tag_headline[0].find_all(text=True)))
        if not text_headline:  # 공백일 경우 기사 제외 처리
            return None
        return text_headline

    @staticmethod
    def get_sentence_from_document(document:BeautifulSoup) -> str:
        tag_content = document.find_all('div', {'id': 'articleBodyContents'})
        text_sentence = ''  # 뉴스 기사 본문 초기화
        text_sentence = text_sentence + ArticleParser.clear_content(str(tag_content[0].find_all(text=True)))
        if not text_sentence or len(text_sentence) < 10:  # 공백일 경우 기사 제외 처리
            return None
        return text_sentence
    
    @staticmethod
    def get_company_from_document(document:BeautifulSoup) -> str:
        tag_company = document.find_all('meta', {'property': 'me2:category1'})
        text_company = ''  # 언론사 초기화
        text_company = text_company + str(tag_company[0].get('content'))
        if not text_company:  # 공백일 경우 기사 제외 처리
            return None
        return text_company
    
    @staticmethod
    def get_imgURL_from_document(document:BeautifulSoup) -> str:
        image_url = document.find('meta', {'property':'me2:image'}).get('content')
        if not image_url:
            return None
        return image_url

    @staticmethod
    def get_datetime_from_document(document:BeautifulSoup) -> str:
        tag_datetime = document.find('span', {'class':'t11'}).text # ex) 2020.11.23 오후 9:06 
        date, ampm, time = tag_datetime.split(' ')
        year, month, day = date.split(".")[:3]
        if ampm == '오후':
            hour = 12 + int(time.split(":")[0])
            if hour == 24:
                hour = 12
        elif ampm == '오전':
            hour = int(time.split(":")[0])
        else: ## 혹시나 오전 오후가 없다면 일단은 그냥 오전이라 생각합시다!
            hour = int(time.split(":")[0])
        minute = int(time.split(":")[1])
        news_time = datetime(year=int(year), month=int(month), day=int(day), hour=int(hour), minute=int(minute), second=0)
        return news_time
    
    @staticmethod
    def find_author(company_string:str) -> str:
        company_list = company_string.split(" ")
        front = 0
        back = len(company_list)-1
        while front <= back:
            if company_list[front] == '기자' or company_list[back] == '기자':
                author_name = company_list[front-1]
                if len(author_name) != 3 :
                    return None
                else:
                    return author_name
            front += 1
            back -= 1
        
        return None

    @staticmethod
    def get_author_name(document:BeautifulSoup) -> str:
        tag_content = document.find_all('div', {'id': 'articleBodyContents'})
        author_name = ArticleParser.parse_author_from_content(text=str(tag_content[0].find_all(text=True)))
        return author_name

    @staticmethod
    def get_category_name(document:BeautifulSoup) -> str:
        category = document.find('meta', {'property':'me2:category2'}).get('content')
        if not category:
            return None
        return category

if __name__ == "__main__":
    from utils import get_document
    url = "https://news.naver.com/main/read.nhn?mode=LSD&mid=shm&sid1=103&oid=022&aid=0003525999"
    url2 = "https://news.naver.com/main/read.nhn?mode=LSD&mid=shm&sid1=104&oid=001&aid=0012034331"
    document = get_document(url)
    document2 = get_document(url2)
    # print(document)
    # 가져와야 하는 것들 category, article_length, image_url, representative
    # headline
    print(ArticleParser.get_headline_from_document(document))
    print(ArticleParser.get_headline_from_document(document2))
    # img_url
    print(ArticleParser.get_imgURL_from_document(document))
    print(ArticleParser.get_imgURL_from_document(document2))
    # created_datetime
    print(ArticleParser.get_datetime_from_document(document))
    print(ArticleParser.get_datetime_from_document(document2))
    # article_company
    print(ArticleParser.get_company_from_document(document))
    print(ArticleParser.get_company_from_document(document2))
    # article_context
    print(ArticleParser.get_sentence_from_document(document))
    print(ArticleParser.get_sentence_from_document(document2))
    # author_name
    print(ArticleParser.get_author_name(document))
    print(ArticleParser.get_author_name(document2))