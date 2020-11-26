from .connection import *


def insert_keyword_keysentence(news_id, key_sentence, keyword):
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

    sql = "INSERT INTO news_extracted VALUES (%s, %s, %s)"
    return execute(sql, (news_id, key_sentence, keyword,))


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
