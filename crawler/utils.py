from time import time
from functools import wraps
from typing import Callable

from bs4 import BeautifulSoup
from requests import HTTPError
import requests
from random import random
import time
from random import uniform
from datetime import datetime

import psycopg2 as pg
from config import CONFIG

def logging_time(original_fn):
    @wraps(original_fn)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = original_fn(*args, **kwargs)
        end_time = time.time()
        print(
            "실행시간[{}]: {} Sec".format(
                original_fn.__name__, round(end_time - start_time, 2)
            )
        )
        return result
    return wrapper

def get_document(url):
    headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Encoding': 'gzip,deflate, br',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36',
        }
    response = requests.get(url, headers=headers)
    document = BeautifulSoup(response.content, 'html.parser')
    return document

class BackOff:
    def __init__(self,
                 max_tries: int = 3,
                 is_success: Callable = None,
                 jitter: bool = True,
                 retry_action: Callable = None,
                 action_when_error: Callable = None,
                 action_when_error_argument: dict = None,
                 method: str = 'Exponential',
                 sleep_time=100):
        self.max_tries = max_tries
        self.is_success = is_success
        self.jitter = jitter
        self.retry_action = retry_action
        self.action_when_error = action_when_error
        self.action_when_error_argument = action_when_error_argument
        self.method = method
        self.sleep_time = sleep_time

    def __call__(self, func):
        @wraps(func)
        def retry(*args, **kwargs):
            tries = 0
            if self.method == 'Exponential':
                sleep_times = exponential_wait()
            elif self.method == 'Simple':
                sleep_times = constant_wait(max_tries=self.max_tries,
                                            sleep_time=self.sleep_time)  # generator / eg. constant_generator
            else:
                print('Available method are Exponential / Simple')
                raise AssignError
            while True:
                tries += 1
                if self.is_success is None:
                    # 에러를 띄울거면 확실히 띄워야함.
                    try:
                        output = func(*args, **kwargs)
                        success = True
                    except HTTPError as e:
                        if e.response.status == 404:
                            output = self.action_when_error(**self.action_when_error_argument)
                            success = True
                        else:
                            output = None
                            success = False
                        if self.retry_action is not None:
                            self.retry_action()
                    except Exception as e:
                        output = None
                        success = False
                        if self.retry_action is not None:
                            self.retry_action()
                else:
                    output = func(*args, **kwargs)
                    success = self.is_success(output)

                if success:
                    return output
                if self.max_tries is not None and tries == self.max_tries:
                    raise MaxTriesExceeded
                sleep_time = next(sleep_times)
                if self.jitter:
                    sleep_time = full_jitter(sleep_time)
                time.sleep(sleep_time)
        return retry

def exponential_wait(base=2, factor=1, max_value=None):
    count = 0
    while True:
        value = factor * base ** count
        if max_value is None or value < max_value:
            yield value
            count += 1
        else:
            yield max_value

def constant_wait(max_tries=5, sleep_time=100):
    for _ in range(max_tries):
        yield sleep_time

def full_jitter(value):
    return uniform(0, value)

class MaxTriesExceeded(Exception):
    def __init__(self, tries):
        self.tries = tries
        self.message = "Max tries error occur now tries is " + str(self.tries)

    def __str__(self):
        return self.message

class AssignError(Exception):
    pass


## Connection for databaese 
class ConnectionStore(object):
    def __init__(self,
                 config_databases,
                 config_host,
                 config_port,
                 config_user,
                 config_password):
        self.config_databases = config_databases
        self.config_host = config_host
        self.config_port = config_port
        self.config_user = config_user
        self.config_password = config_password
        self.connection = self.connect_databases()
        self.execution_date = datetime.utcnow().strftime('%Y-%m-%d')

    def connect_databases(self):
        conn_postgre = pg.connect(host=self.config_host,
                               user=self.config_user,
                               password=self.config_password,
                               port=self.config_port,
                               database=self.config_databases,)
        conn_postgre.autocommit = True
        return conn_postgre

    def restore_connection(self):
        self.close_connection()
        return self.connect_databases

    def close_connection(self):
        self.connection.close()
    
    def execute_query(self, query):
        # @BackOff(max_tries=3, 
        #          retry_action=self.restore_connection,
        #          method='Exponential', 
        #          sleep_time=2)
        def wrapper(*args, **kwargs):
            connection=self.connection
            cur=connection.cursor()
            cur.execute(query)
            return cur
        return wrapper()
    
    ## example format : INSERT INTO Customers (CustomerName, City, Country) VALUES ('Cardinal', 'Stavanger', 'Norway');
    ## origin val : columns or table.columns /  new val: excluded.columns
    def upsert(self, table_name, columns_list, values_list, action_on_conflict='update'):
        values_list = list(set(values_list))
        values = ','.join(values_list)
        if action_on_conflict == 'update':
            query = f"""
            INSERT INTO {table_name} ({','.join(columns_list)}) 
            VALUES {values}
            ON CONFLICT ON CONSTRAINT {table_name}_unique
            DO UPDATE
            SET ({','.join(columns_list)}) = ({','.join(['excluded.' + x for x in columns_list])})
            """
        elif action_on_conflict == 'nothing':
            query = f"""
            INSERT INTO {table_name} ({','.join(columns_list)})
            VALUES {values}
            ON CONFLICT ON CONSTRAINT {table_name}_unique
            DO NOTHING;
            """
        else:
            query = f"""
            INSERT INTO {table_name} ({','.join(columns_list)})
            VALUES {values}
            ON CONFLICT ON CONSTRAINT {table_name}_unique
            DO UPDATE
            SET ({','.join(columns_list)}) = ({','.join(['excluded.' + x for x in columns_list])})
            """
        self.execute_query(query)

class DataManager:
    def __init__(self):
        pass
    
    @staticmethod
    def values_query_formmater(values_list):
        format_str_values = ["NULL" if x is None else "'" + str(x).replace("'", "''") + "'" if x != '' else "NULL"
                            for x in values_list]
        fomat_query_values_with_parenthesis = '(' + ','.join(format_str_values) + ')'
        return fomat_query_values_with_parenthesis   
    
    @staticmethod
    def get_query_format_from_dict(columns_list, values_list_dict:list) -> list:
        # make dict to query format
        values_list = []
        while len(values_list_dict) != 0:
            val = values_list_dict.pop()
            val = [val.get(key) for key in columns_list]
            values_list.append(val)
        values_list = [DataManager.values_query_formmater(val) for val in values_list]
        return values_list

if __name__ == "__main__":
    postgresql_connection = ConnectionStore(CONFIG["postgresql_database"],
                                   CONFIG["postgresql_host"],
                                   CONFIG["postgresql_port"],
                                   CONFIG["postgresql_user"],
                                   CONFIG["postgresql_password"],)
    print(postgresql_connection.execute_query("""SELECT 'hello world!!'""").fetchone()[0])
