CREATE TABLE news_collecting_log
(
    created_date                    TIMESTAMP,
    category                        VARCHAR(50),
    article_headline                VARCHAR(1000),
    article_company                 VARCHAR(100),
    article_url                     VARCHAR(1000),
    reporter_name                   VARCHAR(100),
    article_length                  INT,
    image_url                       VARCHAR(1000),
    representative                  BOOLEAN      DEFAULT FALSE,
    download_date                   TIMESTAMP,
    update_date                     TIMESTAMP DEFAULT now(),
    PRIMARY KEY (created_date, category, article_url)
    );

-- new desk crawling log
CREATE TABLE news_collecting_log
(
    news_id                         BIGINT,
    retrieve_datetime               TIMESTAMP,
    download_datetime               TIMESTAMP,
    update_datetime                 TIMESTAMP DEFAULT now(),
    download_execution_time         TIME,
    error_message                   TEXT
    PRIMARY KEY (created_date, category, article_url)
    );