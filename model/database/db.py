from .connection import *


def update_keyword_keysentence(news_id, key_sentence, keyword):
    """
    input :
        news_id : 뉴스 아이디 
        key_sentence : 중요 문장 (varchar)
        keyword : 중요 단어 (varchar)
    return :
        boolean
    function :
        news_extracted 테이블에 분석된 키워드를 삽입
    """

    if is_analysis(news_id):
        sql = "UPDATE news_extracted SET key_sentence = %s, keyword = %s where news_id = %s"
        return execute(sql, (key_sentence, keyword, news_id, ))
    else:
        return

def is_analysis(news_id):
    """
    input :
        news_id : 뉴스 아이디 
    return :
        boolean
    function :
        이 뉴스가 키워드 분석이 되었는지 여부
    """

    sql = "SELECT news_id FROM news_extracted WHERE news_id = %s"

    if queryone(sql, (news_id,)):
        return True
    else:
        return False


def newly_inserted_list():
    """
    return :
        newly inserted news list
    function : 
        새롭게 keyword analysis 해야 할 리스트
    """
    sql = "SELECT nm.news_id, date(nm.created_datetime), nm.category \
        FROM news_metadata nm LEFT OUTER JOIN news_extracted ne \
        ON ne.news_id = nm.news_id WHERE ne.keyword IS NULL"

    return queryall(sql)


def get_keyword_keysentence(news_id):

    """
    input :
        news_id : 뉴스 아이디 
    return :
        keyword, keysentence info
    function :
        중요 키워드와 문장을 리턴
    """
    sql = "SELECT * FROM news_extracted WHERE news_id = %s"

    return queryone(sql, (news_id,))


def get_preprocessed_news_ids():

    """
    input :
        news_id : 뉴스 아이디 
    return :
        keyword, keysentence info
    function :
        중요 키워드와 문장을 리턴
    """
    sql = "SELECT * FROM news_extracted"

    return queryall(sql)


def get_header(news_id):

    sql = "SELECT ne.news_id, nm.article_headline, ne.keyword, ne.key_sentence FROM (SELECT news_id, keyword, key_sentence FROM news_extracted WHERE news_id = %s) AS ne\
            LEFT JOIN news_metadata nm on ne.news_id = nm.news_id"

    return queryone(sql, (news_id,))


def select_not_null(category, date):
    sql = "SELECT news_id,key_sentence, keyword, date(created_datetime) as date from news_extracted \
        where key_sentence != '' and category = %s and date(created_datetime) = %s;"
    return queryall(sql, (category, date, ))


def get_current_articles(category, num):

    sql = "SELECT news_id,key_sentence, keyword, date(created_datetime) as date FROM news_extracted WHERE category = %s and key_sentence <> '' ORDER BY created_datetime DESC LIMIT %s;"
    return queryall(sql, (category, num, ))

def insert_representatives(news_id):
    sql = "INSERT INTO news_service (created_datetime, image_url, article_headline, article_url, category, news_id) \
        SELECT created_datetime, image_url, article_headline, article_url, category, news_id FROM news_metadata where news_id = %s;"
    return execute(sql, (news_id, ))
    
def update_representatives(news_id):
    sql = "UPDATE news_service SET key_sentence = (SELECT key_sentence FROM news_extracted WHERE news_id = %s) where news_id = %s;"
    return execute(sql, (news_id, news_id ))