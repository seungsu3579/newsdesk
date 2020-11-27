from datetime import date, timedelta, datetime
import time
import concurrent.futures

from crawler.articlecrawler import ArticleCrawler
from utils import ConnectionStore
from config import CONFIG

postgre_connection = ConnectionStore(CONFIG["postgresql_database"],
                                   CONFIG["postgresql_host"],
                                   CONFIG["postgresql_port"],
                                   CONFIG["postgresql_user"],
                                   CONFIG["postgresql_password"],)
Crawler = ArticleCrawler(connection=postgre_connection)

for d in range(1,30):
    target_date = datetime.now() - timedelta(days=d)
    year, month, day = target_date.year, target_date.month, target_date.day
    Crawler.set_date_range(year, month, day)

    if month != 11:
        break

    print(year, month, day, 'starts')
    
    for category in ['정치', '경제', '사회', '생활문화', '세계', 'IT과학', '오피니언']:
        Crawler.make_crawling_log(category)
        print(f"{category} : {year}-{month}-{day} is DONE")
        time.sleep(10)

    print(year, month, day, 'is done')
    time.sleep(30)