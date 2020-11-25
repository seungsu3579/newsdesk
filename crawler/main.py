from datetime import date, timedelta, datetime
import time
import concurrent.futures

from crawler.articlecrawler import ArticleCrawler
from utils import ConnectionStore
from config import CONFIG

if __name__ == '__main__':


    postgre_connection = ConnectionStore(CONFIG["postgresql_database"],
                                   CONFIG["postgresql_host"],
                                   CONFIG["postgresql_port"],
                                   CONFIG["postgresql_user"],
                                   CONFIG["postgresql_password"],)
    Crawler = ArticleCrawler(config=CONFIG, connection=postgre_connection)

    for d in range(1,30):
        target_date = datetime.now() - timedelta(days=d)
        year, month, day = target_date.year, target_date.month, target_date.day
        Crawler.set_date_range(year, month, day)

        if month != 11:
            continue
        if day >= 22:
            continue

        print(year, month, day, 'starts')
        
        Crawler.crawl_all_categries(['정치', '경제'])
        print(year, month, day, 'is done')
        time.sleep(30)



    