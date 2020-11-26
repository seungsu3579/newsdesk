import pymysql
import sys
from config import CONFIG

# PyMySQL Documentation: https://pymysql.readthedocs.io/en/latest/index.html


def _connect():
    connection = pymysql.connect(
        user=CONFIG["RDS_configure"]["postgresql_user"],
        password=CONFIG["RDS_configure"]["postgresql_password"],
        host=CONFIG["RDS_configure"]["postgresql_host"],
        port=CONFIG["RDS_configure"]["postgresql_port"],
        database=CONFIG["RDS_configure"]["postgresql_database"],
        cursorclass=pymysql.cursors.DictCursor,
    )

    return connection


def queryone(sql, fmt=tuple()):
    """쿼리문을 실행시키고, 찾은 항목 중 첫번째 튜플 반환"""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(sql, fmt)
        return cur.fetchone()
    except Exception as e:
        print(e)
        raise e
    finally:
        cur.close()
        conn.close()


def queryall(sql, fmt=tuple()):
    """쿼리문을 실행시키고, 찾은 항목 전체 튜플 리스트 반환"""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(sql, fmt)
        return cur.fetchall()
    except Exception as e:
        print(e)
        raise e
    finally:
        cur.close()
        conn.close()


def execute(sql, fmt=tuple()):
    """쿼리문을 실행시키고, 변화를 저장"""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(sql, fmt)
        conn.commit()
    except Exception as e:
        print(e)
        raise e
    finally:
        cur.close()
        conn.close()


def callproc(sql, fmt=tuple()):
    """프로시져를 실행시키고, 변화를 저장"""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.callproc(sql, fmt)
        message = cur.fetchall()
        conn.commit()
        return message
    except Exception as e:
        print(e)
        raise e
    finally:
        cur.close()
        conn.close()
